package detector

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/veritrace-platform/telemetry-stream-service/internal/envelope"
	"github.com/veritrace-platform/telemetry-stream-service/internal/exposure"
	"github.com/veritrace-platform/telemetry-stream-service/internal/forecast"
	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
	"github.com/veritrace-platform/telemetry-stream-service/internal/warning"
)

// History is how far back the database is read to rebuild an episode (cold-chain-monitoring.md §6). The forecast is
// rebuilt from the readings of forecast.LevelWindow (§7.4).
const History = 60 * time.Second

// Producer writes records and waits for them to be acknowledged, as *kgo.Client does.
type Producer interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
}

// Input is a stored reading and the trace of the record it came from.
type Input struct {
	Reading     reading.Reading
	Traceparent string
}

// Detector keeps the state of each SSCC of the partitions it consumes and evaluates batches of stored readings: the
// breach episode (cold-chain-monitoring.md §4), the excursion forecast (§7), and the shelf-life assessments of a
// lasting breach (§8.3). The state lives in memory and is rebuilt from the database at the first reading of an SSCC
// after a start, a rebalance, or a failed batch.
type Detector struct {
	pool     *pgxpool.Pool
	producer Producer
	logger   *slog.Logger
	now      func() time.Time
	latency  prometheus.Histogram

	mu       sync.Mutex
	trackers map[string]*tracker
}

// tracker is the state of one SSCC.
type tracker struct {
	episode  *Episode
	forecast *forecast.Forecaster
	// assessed is the device time of the last shelf-life assessment of a lasting breach; zero until the first.
	assessed time.Time
}

// New returns a detector. Its metrics are registered with registerer.
func New(pool *pgxpool.Pool, producer Producer, logger *slog.Logger, registerer prometheus.Registerer,
	now func() time.Time,
) *Detector {
	latency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "veritrace_telemetry_breach_detection_latency_seconds",
		Help:    "Time from receiving the reading that confirms a breach to producing cold_chain.breach_confirmed.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	})
	registerer.MustRegister(latency)
	return &Detector{
		pool: pool, producer: producer, logger: logger, now: now, latency: latency, trackers: map[string]*tracker{},
	}
}

// Reset drops the state of every SSCC, for example when partitions are revoked. Each is rebuilt at its next reading.
func (d *Detector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.trackers)
}

// Evaluate runs a batch of stored readings through their SSCC's episodes, in device-time order. Readings of
// shipments that the projection does not know, or that are not CREATED or IN_TRANSIT, are not evaluated
// (cold-chain-monitoring.md §3).
//
// Incidents and warnings are written in one transaction, and their events are produced before it commits: an event
// may be produced again if the batch fails and is processed again, but it is never lost. Events derive their IDs
// from what they describe, so a repeated event is recognizable. A failed batch drops the state of every SSCC, so
// that the next attempt rebuilds it from what the database holds.
func (d *Detector) Evaluate(ctx context.Context, inputs []Input) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.evaluate(ctx, inputs); err != nil {
		clear(d.trackers)
		return err
	}
	return nil
}

// batch collects the events of one evaluation.
type batch struct {
	records []*kgo.Record
	// received holds when each confirming reading was received, for the latency metric.
	received []time.Time
}

func (d *Detector) evaluate(ctx context.Context, inputs []Input) error {
	bySSCC := map[string][]Input{}
	for _, in := range inputs {
		bySSCC[in.Reading.SSCC] = append(bySSCC[in.Reading.SSCC], in)
	}
	ssccs := slices.Sorted(maps.Keys(bySSCC))
	if len(ssccs) == 0 {
		return nil
	}

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// The projections stay as read until the incidents of the batch are recorded (projection.LockForDetection).
	shipments, err := projection.LockForDetection(ctx, tx, ssccs)
	if err != nil {
		return err
	}

	var b batch
	for _, sscc := range ssccs {
		shipment, known := shipments[sscc]
		if !known || !shipment.Monitored() {
			delete(d.trackers, sscc)
			continue
		}
		group := bySSCC[sscc]
		slices.SortStableFunc(group, func(a, b Input) int {
			return cmp.Or(a.Reading.RecordedAt.Compare(b.Reading.RecordedAt), cmp.Compare(a.Reading.DeviceID, b.Reading.DeviceID))
		})
		tr, ok := d.trackers[sscc]
		if !ok {
			if tr, err = d.rebuild(ctx, tx, shipment, group[0].Reading.RecordedAt, &b); err != nil {
				return err
			}
			d.trackers[sscc] = tr
		}
		for _, in := range group {
			if err := d.step(ctx, tx, shipment, tr, in, &b); err != nil {
				return err
			}
		}
	}

	if len(b.records) > 0 {
		if err := d.producer.ProduceSync(ctx, b.records...).FirstErr(); err != nil {
			return fmt.Errorf("produce incident events: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	for _, received := range b.received {
		d.latency.Observe(d.now().Sub(received).Seconds())
	}
	return nil
}

// rebuild returns the state of a shipment as the database records it before first, the time of the first new
// reading. The episode starts from the SSCC's latest incident, and the readings of the last minute are evaluated
// again; their changes are applied too, so an incident that a failed batch confirmed or resolved without committing
// is caught up, and incidents that exist are left as they are. The forecast continues the SSCC's open warning, and
// the readings of the last 30 minutes fill its window and operating level without issuing or clearing warnings
// (cold-chain-monitoring.md §7.4).
func (d *Detector) rebuild(ctx context.Context, tx pgx.Tx, s projection.Shipment, first time.Time, b *batch) (*tracker, error) {
	tr := &tracker{
		episode:  NewEpisode(s.SSCC, s.ShipmentID, Bounds{Min: s.MinTempCelsius, Max: s.MaxTempCelsius}),
		forecast: forecast.New(forecast.Bounds{Min: s.MinTempCelsius, Max: s.MaxTempCelsius}),
	}
	from := first.Add(-History)
	lastBefore, _, err := reading.LastBefore(ctx, tx, s.SSCC, from)
	if err != nil {
		return nil, err
	}
	latest, found, err := incident.NewStore(tx).Latest(ctx, s.SSCC)
	if err != nil {
		return nil, err
	}
	if found {
		tr.episode.Seed(lastBefore, &latest)
	} else {
		tr.episode.Seed(lastBefore, nil)
	}
	open, found, err := warning.NewStore(tx).Open(ctx, s.SSCC)
	if err != nil {
		return nil, err
	}
	if found {
		w := open.Forecast()
		tr.forecast.Seed(&w)
	}
	history, err := reading.Between(ctx, tx, s.SSCC, first.Add(-forecast.LevelWindow), first)
	if err != nil {
		return nil, err
	}
	for _, r := range history {
		tr.forecast.Observe(forecastReading(r))
		if r.RecordedAt.Before(from) {
			continue
		}
		changes, _ := tr.episode.Step(r)
		if err := d.apply(ctx, tx, s, tr, changes, Input{Reading: r}, b); err != nil {
			return nil, err
		}
	}
	return tr, nil
}

// step evaluates one new reading of a shipment: the breach rule, then the forecast, then, while a breach lasts, the
// shelf life every exposure.AssessEvery of device time.
func (d *Detector) step(ctx context.Context, tx pgx.Tx, s projection.Shipment, tr *tracker, in Input, b *batch) error {
	changes, evaluated := tr.episode.Step(in.Reading)
	if err := d.apply(ctx, tx, s, tr, changes, in, b); err != nil {
		return err
	}
	if !evaluated {
		return nil
	}
	switch decision, w := tr.forecast.Step(forecastReading(in.Reading)); decision {
	case forecast.Issue:
		if err := d.issue(ctx, tx, s, w, in, b); err != nil {
			return err
		}
	case forecast.Clear:
		if err := d.clear(ctx, tx, s, w, in, b); err != nil {
			return err
		}
	case forecast.None:
	}
	if tr.episode.Breached() {
		at := in.Reading.RecordedAt
		switch {
		case tr.assessed.IsZero():
			tr.assessed = at
		case at.Sub(tr.assessed) >= exposure.AssessEvery:
			tr.assessed = at
			return d.reassess(ctx, tx, s, at, in, b)
		}
	}
	return nil
}

// apply writes the changes of one reading and collects the events of those that took effect: a confirmation
// that inserted the incident, and a resolution that ended it. A change that finds the incident already in that
// state was applied before and produces nothing. A confirmation also closes the warning that preceded the breach,
// and a resolution assesses the shelf life that the breach cost.
func (d *Detector) apply(ctx context.Context, tx pgx.Tx, s projection.Shipment, tr *tracker, changes []Change, in Input,
	b *batch,
) error {
	store := incident.NewStore(tx)
	for _, c := range changes {
		switch c.Kind {
		case Confirmed:
			i := c.Incident
			var err error
			if i.IncidentHash, err = i.Confirmation().Hash(); err != nil {
				return err
			}
			inserted, err := store.Insert(ctx, i)
			if err != nil {
				return err
			}
			tr.assessed = i.ConfirmedAt
			if err := d.closeBreached(ctx, tx, s, tr, i, in, b); err != nil {
				return err
			}
			if !inserted {
				continue
			}
			event, err := incident.ConfirmedEvent(i, s.Status)
			if err != nil {
				return err
			}
			if err := b.add(event.Event, incident.Topic, in); err != nil {
				return err
			}
			b.received = append(b.received, in.Reading.ReceivedAt)
			d.logger.InfoContext(ctx, "breach confirmed", slog.String("sscc", i.SSCC), slog.String("incident_id", i.ID.String()),
				slog.Float64("temperature_celsius", i.TriggerTemperatureCelsius))
		case Extended:
			if _, err := store.Extend(ctx, c.Incident.ID, c.Incident.ExtremeTemperatureCelsius); err != nil {
				return err
			}
		case Resolved:
			resolved, err := store.Resolve(ctx, c.Incident)
			if err != nil {
				return err
			}
			if !resolved {
				continue
			}
			event, err := incident.ResolvedEvent(c.Incident, s.Status)
			if err != nil {
				return err
			}
			if err := b.add(event.Event, incident.Topic, in); err != nil {
				return err
			}
			d.logger.InfoContext(ctx, "breach resolved", slog.String("sscc", c.Incident.SSCC),
				slog.String("incident_id", c.Incident.ID.String()), slog.Int("duration_seconds", *c.Incident.DurationSeconds))
			if err := d.reassess(ctx, tx, s, *c.Incident.EndedAt, in, b); err != nil {
				return err
			}
		}
	}
	return nil
}

// closeBreached closes the SSCC's open warning as BREACHED by a confirmed incident, if it was issued before the
// incident's episode started (cold-chain-monitoring.md §7.2).
func (d *Detector) closeBreached(ctx context.Context, tx pgx.Tx, s projection.Shipment, tr *tracker, i incident.Incident,
	in Input, b *batch,
) error {
	if open, ok := tr.forecast.Open(); ok && !open.IssuedAt.After(i.StartedAt) {
		tr.forecast.Close()
	}
	w, closed, err := warning.NewStore(tx).CloseBreached(ctx, s.SSCC, i.ID, i.StartedAt, i.ConfirmedAt)
	if err != nil || !closed {
		return err
	}
	event, err := warning.ClosedEvent(w, s.Status)
	if err != nil {
		return err
	}
	d.logger.InfoContext(ctx, "excursion warning breached", slog.String("sscc", s.SSCC), slog.String("warning_id", w.ID.String()),
		slog.String("incident_id", i.ID.String()), slog.Int("lead_time_seconds", *w.LeadTimeSeconds))
	return b.add(event, envelope.TopicForecasts, in)
}

// issue records a warning that the forecast issued and collects its event.
func (d *Detector) issue(ctx context.Context, tx pgx.Tx, s projection.Shipment, fw forecast.Warning, in Input, b *batch) error {
	w := warning.FromForecast(fw, s.SSCC, s.ShipmentID)
	inserted, err := warning.NewStore(tx).Insert(ctx, w)
	if err != nil || !inserted {
		return err
	}
	event, err := warning.IssuedEvent(w, s.Status)
	if err != nil {
		return err
	}
	d.logger.InfoContext(ctx, "excursion warning issued", slog.String("sscc", s.SSCC), slog.String("warning_id", w.ID.String()),
		slog.String("bound", string(w.Bound)), slog.Float64("slope_celsius_per_minute", w.SlopeCelsiusPerMinute),
		slog.Time("predicted_excursion_at", w.PredictedExcursionAt))
	return b.add(event, envelope.TopicForecasts, in)
}

// clear closes a warning that the forecast cleared, at the reading that cleared it, and collects its event.
func (d *Detector) clear(ctx context.Context, tx pgx.Tx, s projection.Shipment, fw forecast.Warning, in Input, b *batch) error {
	w, closed, err := warning.NewStore(tx).Close(ctx, warning.NewID(s.SSCC, fw.IssuedAt), warning.Cleared, in.Reading.RecordedAt)
	if err != nil || !closed {
		return err
	}
	event, err := warning.ClosedEvent(w, s.Status)
	if err != nil {
		return err
	}
	d.logger.InfoContext(ctx, "excursion warning cleared", slog.String("sscc", s.SSCC), slog.String("warning_id", w.ID.String()))
	return b.add(event, envelope.TopicForecasts, in)
}

// reassess assesses the shipment's shelf life as of at and collects a shelf-life alert, if the status got worse.
func (d *Detector) reassess(ctx context.Context, tx pgx.Tx, s projection.Shipment, at time.Time, in Input, b *batch) error {
	event, err := exposure.Reassess(ctx, tx, s.Exposure(), at)
	if err != nil || event == nil {
		return err
	}
	d.logger.InfoContext(ctx, "shelf life reduced", slog.String("sscc", s.SSCC), slog.String("data", string(event.Data)))
	return b.add(*event, envelope.TopicForecasts, in)
}

// add collects the record of an event on topic, in the trace of the reading that caused it.
func (b *batch) add(e envelope.Event, topic string, in Input) error {
	record, err := e.Record(topic, childTrace(in.Traceparent))
	if err != nil {
		return err
	}
	b.records = append(b.records, record)
	return nil
}

// forecastReading returns a stored reading as the forecaster sees it.
func forecastReading(r reading.Reading) forecast.Reading {
	return forecast.Reading{At: r.RecordedAt, Celsius: r.TemperatureCelsius, DeviceID: r.DeviceID}
}

// childTrace continues the reading's trace, or starts one for a reading read back from the database.
func childTrace(traceparent string) string {
	if tc, ok := tracecontext.Parse(traceparent); ok {
		return tc.Child().Traceparent()
	}
	return tracecontext.New().Traceparent()
}

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

	"github.com/veritrace-platform/telemetry-stream-service/internal/incident"
	"github.com/veritrace-platform/telemetry-stream-service/internal/platform/tracecontext"
	"github.com/veritrace-platform/telemetry-stream-service/internal/projection"
	"github.com/veritrace-platform/telemetry-stream-service/internal/reading"
)

// History is how far back the database is read to rebuild an episode (cold-chain-monitoring.md §6).
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

// Detector keeps an episode per SSCC of the partitions it consumes and evaluates batches of stored readings.
// Episodes live in memory and are rebuilt from the database at the first reading of an SSCC after a start, a
// rebalance, or a failed batch.
type Detector struct {
	pool     *pgxpool.Pool
	producer Producer
	logger   *slog.Logger
	now      func() time.Time
	latency  prometheus.Histogram

	mu       sync.Mutex
	episodes map[string]*Episode
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
		pool: pool, producer: producer, logger: logger, now: now, latency: latency, episodes: map[string]*Episode{},
	}
}

// Reset drops every episode, for example when partitions are revoked. Each is rebuilt at its next reading.
func (d *Detector) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.episodes)
}

// Evaluate runs a batch of stored readings through their SSCC's episodes, in device-time order. Readings of
// shipments that the projection does not know, or that are not CREATED or IN_TRANSIT, are not evaluated
// (cold-chain-monitoring.md §3).
//
// Incidents are written in one transaction, and their events are produced before it commits: an event may be
// produced again if the batch fails and is processed again, but it is never lost. Events derive their IDs from
// the incident, so a repeated event is recognizable. A failed batch drops every episode, so that the next attempt
// rebuilds them from what the database holds.
func (d *Detector) Evaluate(ctx context.Context, inputs []Input) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.evaluate(ctx, inputs); err != nil {
		clear(d.episodes)
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
	shipments, err := projection.LookUp(ctx, tx, ssccs)
	if err != nil {
		return err
	}

	var b batch
	for _, sscc := range ssccs {
		shipment, known := shipments[sscc]
		if !known || !shipment.Monitored() {
			delete(d.episodes, sscc)
			continue
		}
		group := bySSCC[sscc]
		slices.SortStableFunc(group, func(a, b Input) int {
			return cmp.Or(a.Reading.RecordedAt.Compare(b.Reading.RecordedAt), cmp.Compare(a.Reading.DeviceID, b.Reading.DeviceID))
		})
		ep, ok := d.episodes[sscc]
		if !ok {
			if ep, err = d.rebuild(ctx, tx, shipment, group[0].Reading.RecordedAt, &b); err != nil {
				return err
			}
			d.episodes[sscc] = ep
		}
		for _, in := range group {
			changes, _ := ep.Step(in.Reading)
			if err := d.apply(ctx, tx, shipment, changes, in, &b); err != nil {
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

// rebuild returns the episode of a shipment as the database records it before first, the time of the first new
// reading: the SSCC's latest incident, and the readings of the last minute, which are evaluated again. Their
// changes are applied too, so an incident that a failed batch confirmed or resolved without committing is caught
// up; incidents that exist are left as they are.
func (d *Detector) rebuild(ctx context.Context, tx pgx.Tx, s projection.Shipment, first time.Time, b *batch) (*Episode, error) {
	ep := NewEpisode(s.SSCC, s.ShipmentID, Bounds{Min: s.MinTempCelsius, Max: s.MaxTempCelsius})
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
		ep.Seed(lastBefore, &latest)
	} else {
		ep.Seed(lastBefore, nil)
	}
	history, err := reading.Between(ctx, tx, s.SSCC, from, first)
	if err != nil {
		return nil, err
	}
	for _, r := range history {
		changes, _ := ep.Step(r)
		if err := d.apply(ctx, tx, s, changes, Input{Reading: r}, b); err != nil {
			return nil, err
		}
	}
	return ep, nil
}

// apply writes the changes of one reading and collects the events of those that took effect: a confirmation
// that inserted the incident, and a resolution that ended it. A change that finds the incident already in that
// state was applied before and produces nothing.
func (d *Detector) apply(ctx context.Context, tx pgx.Tx, s projection.Shipment, changes []Change, in Input, b *batch) error {
	store := incident.NewStore(tx)
	for _, c := range changes {
		var (
			event incident.Event
			err   error
		)
		switch c.Kind {
		case Confirmed:
			i := c.Incident
			if i.IncidentHash, err = i.Confirmation().Hash(); err != nil {
				return err
			}
			inserted, err := store.Insert(ctx, i)
			if err != nil || !inserted {
				return err
			}
			if event, err = incident.ConfirmedEvent(i, s.Status); err != nil {
				return err
			}
			b.received = append(b.received, in.Reading.ReceivedAt)
			d.logger.InfoContext(ctx, "breach confirmed", slog.String("sscc", i.SSCC), slog.String("incident_id", i.ID.String()),
				slog.Float64("temperature_celsius", i.TriggerTemperatureCelsius))
		case Extended:
			if _, err := store.Extend(ctx, c.Incident.ID, c.Incident.ExtremeTemperatureCelsius); err != nil {
				return err
			}
			continue
		case Resolved:
			resolved, err := store.Resolve(ctx, c.Incident)
			if err != nil || !resolved {
				return err
			}
			if event, err = incident.ResolvedEvent(c.Incident, s.Status); err != nil {
				return err
			}
			d.logger.InfoContext(ctx, "breach resolved", slog.String("sscc", c.Incident.SSCC),
				slog.String("incident_id", c.Incident.ID.String()), slog.Int("duration_seconds", *c.Incident.DurationSeconds))
		}
		record, err := event.Record(childTrace(in.Traceparent))
		if err != nil {
			return err
		}
		b.records = append(b.records, record)
	}
	return nil
}

// childTrace continues the reading's trace, or starts one for a reading read back from the database.
func childTrace(traceparent string) string {
	if tc, ok := tracecontext.Parse(traceparent); ok {
		return tc.Child().Traceparent()
	}
	return tracecontext.New().Traceparent()
}

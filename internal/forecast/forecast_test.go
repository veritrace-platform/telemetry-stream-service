package forecast_test

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/veritrace-platform/telemetry-stream-service/internal/forecast"
)

var (
	t0      = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	chilled = forecast.Bounds{Min: 2, Max: 8}
)

func TestFitRecoversALine(t *testing.T) {
	var points []forecast.Point
	for i := range 25 {
		at := t0.Add(time.Duration(i) * 5 * time.Second)
		points = append(points, forecast.Point{At: at, Celsius: 4 + 0.5*at.Sub(t0).Minutes()})
	}
	trend, ok := forecast.Fit(points)
	if !ok || math.Abs(trend.Slope-0.5) > 1e-9 || math.Abs(trend.Fitted-5) > 1e-9 || trend.TStatistic < 1e6 ||
		trend.Readings != 25 || trend.Span != 2*time.Minute || !trend.At.Equal(t0.Add(2*time.Minute)) {
		t.Fatalf("Fit() = %+v, %v; want slope 0.5 °C/min ending at 5 °C", trend, ok)
	}
	if bound, moving := trend.Toward(); !moving || bound != forecast.Max {
		t.Errorf("Toward() = %s, %v", bound, moving)
	}
	if tau, ok := trend.TimeTo(chilled, forecast.Max); !ok || tau != 6*time.Minute {
		t.Errorf("TimeTo(max) = %s, %v; want 6m", tau, ok)
	}
	if _, ok := trend.TimeTo(chilled, forecast.Min); ok {
		t.Error("a rising trend reaches the minimum")
	}

	if _, ok := forecast.Fit(points[:11]); ok {
		t.Error("Fit() of 11 points succeeded")
	}
	close := make([]forecast.Point, 12)
	for i := range close {
		close[i] = forecast.Point{At: t0.Add(time.Duration(i) * time.Second), Celsius: 4}
	}
	if _, ok := forecast.Fit(close); ok {
		t.Error("Fit() of 11 seconds succeeded")
	}
}

func TestFitMeasuresSignificance(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	noisy := func(slope float64) forecast.Trend {
		var points []forecast.Point
		for i := range 25 {
			at := t0.Add(time.Duration(i) * 5 * time.Second)
			points = append(points, forecast.Point{At: at, Celsius: 5 + slope*at.Sub(t0).Minutes() + rng.NormFloat64()*0.1})
		}
		trend, _ := forecast.Fit(points)
		return trend
	}
	if flat := noisy(0); math.Abs(flat.TStatistic) >= forecast.MinTStatistic {
		t.Errorf("noise alone: t = %.2f, slope %.3f", flat.TStatistic, flat.Slope)
	}
	if rising := noisy(0.5); rising.TStatistic < 10 {
		t.Errorf("0.5 °C/min under noise of 0.1 °C: t = %.2f", rising.TStatistic)
	}
	if fitted := (forecast.Trend{Slope: 0.5, Fitted: 9}); true {
		if tau, ok := fitted.TimeTo(chilled, forecast.Max); !ok || tau != 0 {
			t.Errorf("past the bound: TimeTo() = %s, %v; want 0", tau, ok)
		}
	}
}

// phase is a stretch of a trace: the temperature moves linearly from start to end.
type phase struct {
	duration   time.Duration
	start, end float64
	silent     bool
}

// trace returns readings every 5 s through the phases, with Gaussian noise of sd, from t0.
func trace(seed uint64, sd float64, phases ...phase) []forecast.Reading {
	rng := rand.New(rand.NewPCG(seed, 7))
	var readings []forecast.Reading
	at := t0
	for _, p := range phases {
		steps := int(p.duration / (5 * time.Second))
		for i := range steps {
			if !p.silent {
				f := float64(i) / float64(steps)
				c := p.start + (p.end-p.start)*f + rng.NormFloat64()*sd
				readings = append(readings, forecast.Reading{At: at, Celsius: math.Round(c*100) / 100, DeviceID: "SIM-1"})
			}
			at = at.Add(5 * time.Second)
		}
	}
	return readings
}

type decided struct {
	decision forecast.Decision
	warning  forecast.Warning
	at       time.Time
}

func run(f *forecast.Forecaster, readings []forecast.Reading) []decided {
	var got []decided
	for _, r := range readings {
		if d, w := f.Step(r); d != forecast.None {
			got = append(got, decided{d, w, r.At})
		}
	}
	return got
}

// firstAbove returns the time of the first reading above limit.
func firstAbove(readings []forecast.Reading, limit float64) time.Time {
	for _, r := range readings {
		if r.Celsius > limit {
			return r.At
		}
	}
	return time.Time{}
}

const steady = 30 * time.Minute

func TestGradualWarmingIsWarnedMinutesAhead(t *testing.T) {
	readings := trace(1, 0.1,
		phase{duration: steady, start: 4.5, end: 4.5},
		phase{duration: 15 * time.Minute, start: 4.5, end: 12},
	)
	got := run(forecast.New(chilled), readings)
	if len(got) == 0 || got[0].decision != forecast.Issue {
		t.Fatalf("decisions = %+v, want a warning", got)
	}
	w := got[0].warning
	excursion := firstAbove(readings, 8)
	lead := excursion.Sub(w.IssuedAt)
	if w.Bound != forecast.Max || w.BoundCelsius != 8 || lead < 3*time.Minute || lead > forecast.Horizon {
		t.Errorf("warning %+v leads the excursion at %s by %s", w, excursion.Format(time.TimeOnly), lead)
	}
	// The window still holds steady readings when the warning is issued, so the slope is low and the excursion
	// is predicted late.
	if errExcursion := w.PredictedExcursionAt.Sub(excursion).Abs(); errExcursion > 150*time.Second {
		t.Errorf("predicted excursion %s, actual %s", w.PredictedExcursionAt.Format(time.TimeOnly), excursion.Format(time.TimeOnly))
	}
	if w.SlopeCelsiusPerMinute < 0.3 || w.SlopeCelsiusPerMinute > 0.6 || w.DeviceID != "SIM-1" {
		t.Errorf("warning = %+v, want a slope near 0.5 °C/min", w)
	}
	// Out of bounds, the warning is neither cleared nor issued again.
	for _, d := range got[1:] {
		t.Errorf("then %+v", d)
	}
}

func TestDoorOpeningIsWarnedAndCleared(t *testing.T) {
	readings := trace(2, 0.1,
		phase{duration: steady, start: 4.5, end: 4.5},
		phase{duration: 3 * time.Minute, start: 4.5, end: 7.2},
		phase{duration: 3 * time.Minute, start: 7.2, end: 4.5},
		phase{duration: 10 * time.Minute, start: 4.5, end: 4.5},
	)
	got := run(forecast.New(chilled), readings)
	if len(got) != 2 || got[0].decision != forecast.Issue || got[0].warning.Bound != forecast.Max ||
		got[1].decision != forecast.Clear || got[1].warning.IssuedAt != got[0].warning.IssuedAt {
		t.Fatalf("decisions = %+v, want a warning about the maximum, then cleared", got)
	}
	// Cleared within two minutes of the peak, and no warning about the minimum while the load recovers.
	peak := t0.Add(steady + 3*time.Minute)
	if cleared := got[1].at.Sub(peak); cleared > 2*time.Minute {
		t.Errorf("cleared %s after the peak", cleared)
	}
}

func TestRecoveryDoesNotWarnAboutTheOtherBound(t *testing.T) {
	readings := trace(3, 0.1,
		phase{duration: steady, start: 4.5, end: 4.5},
		phase{duration: 2 * time.Minute, start: 10, end: 10.5},
		phase{duration: 3 * time.Minute, start: 10.5, end: 4.5},
		phase{duration: 10 * time.Minute, start: 4.5, end: 4.5},
	)
	for _, d := range run(forecast.New(chilled), readings) {
		t.Errorf("recovery: %+v", d)
	}
}

func TestNoiseNearTheBoundIsNoTrend(t *testing.T) {
	readings := trace(4, 0.15, phase{duration: 2 * steady, start: 7.6, end: 7.6})
	for _, d := range run(forecast.New(chilled), readings) {
		t.Errorf("noise at 7.6 °C: %+v", d)
	}
}

func TestFallingTemperatureWarnsAboutTheMinimum(t *testing.T) {
	readings := trace(5, 0.1,
		phase{duration: steady, start: 4.5, end: 4.5},
		phase{duration: 10 * time.Minute, start: 4.5, end: 0},
	)
	got := run(forecast.New(chilled), readings)
	if len(got) != 1 || got[0].warning.Bound != forecast.Min || got[0].warning.BoundCelsius != 2 {
		t.Fatalf("decisions = %+v, want one warning about the minimum", got)
	}
}

func TestGapEmptiesTheWindow(t *testing.T) {
	readings := trace(7, 0,
		phase{duration: steady, start: 4.5, end: 4.5},
		phase{duration: 20 * time.Second, silent: true},
		phase{duration: 55 * time.Second, start: 4.5, end: 6.7},
	)
	// The steep rise after the gap would make a trend with the steady readings before it, but the gap empties the
	// window, and the rise alone has too few readings.
	for _, d := range run(forecast.New(chilled), readings) {
		t.Errorf("across a gap: %+v", d)
	}
}

func TestOrderingAndSeed(t *testing.T) {
	f := forecast.New(chilled)
	r := forecast.Reading{At: t0, Celsius: 4}
	if !f.Observe(r) || f.Observe(r) {
		t.Error("Observe() accepted a reading that is not after the last")
	}
	if d, _ := f.Step(forecast.Reading{At: t0.Add(-time.Second), Celsius: 9}); d != forecast.None {
		t.Error("Step() decided on an old reading")
	}
	open := forecast.Warning{IssuedAt: t0, Bound: forecast.Max, BoundCelsius: 8}
	f.Seed(&open)
	if w, ok := f.Open(); !ok || w.IssuedAt != t0 {
		t.Errorf("Open() after Seed() = %+v, %v", w, ok)
	}
	f.Close()
	if _, ok := f.Open(); ok {
		t.Error("Open() after Close() reports a warning")
	}

	// A seeded warning clears like an issued one: a flat trend no longer approaches the bound.
	g := forecast.New(chilled)
	g.Seed(&open)
	got := run(g, trace(8, 0.05, phase{duration: 5 * time.Minute, start: 4.5, end: 4.5}))
	if len(got) != 1 || got[0].decision != forecast.Clear {
		t.Errorf("seeded warning on a flat trend: %+v", got)
	}
}

// TestRulesHoldAcrossNoise runs each shape under 50 draws of sensor noise: what a shape yields must not depend on
// one lucky draw.
func TestRulesHoldAcrossNoise(t *testing.T) {
	shapes := []struct {
		name   string
		sd     float64
		phases []phase
		want   string // the decisions, I for issued (with the bound) and C for cleared
	}{
		{"gradual warming", 0.1, []phase{{duration: steady, start: 4.5, end: 4.5}, {duration: 15 * time.Minute, start: 4.5, end: 12}}, "IMAX"},
		{"door opening", 0.1, []phase{
			{duration: steady, start: 4.5, end: 4.5}, {duration: 3 * time.Minute, start: 4.5, end: 7.2},
			{duration: 3 * time.Minute, start: 7.2, end: 4.5}, {duration: 10 * time.Minute, start: 4.5, end: 4.5},
		}, "IMAXC"},
		{"recovery after a breach", 0.1, []phase{
			{duration: steady, start: 4.5, end: 4.5}, {duration: 2 * time.Minute, start: 10, end: 10.5},
			{duration: 3 * time.Minute, start: 10.5, end: 4.5}, {duration: 10 * time.Minute, start: 4.5, end: 4.5},
		}, ""},
		{"noise near the bound", 0.15, []phase{{duration: 2 * steady, start: 7.6, end: 7.6}}, ""},
		{"falling", 0.1, []phase{{duration: steady, start: 4.5, end: 4.5}, {duration: 10 * time.Minute, start: 4.5, end: 0}}, "IMIN"},
	}
	for _, shape := range shapes {
		for seed := range uint64(50) {
			var got string
			for _, d := range run(forecast.New(chilled), trace(1000+seed, shape.sd, shape.phases...)) {
				if d.decision == forecast.Issue {
					got += "I" + string(d.warning.Bound)
				} else {
					got += "C"
				}
			}
			if got != shape.want {
				t.Errorf("%s, noise draw %d: decisions %q, want %q", shape.name, seed, got, shape.want)
			}
		}
	}
}

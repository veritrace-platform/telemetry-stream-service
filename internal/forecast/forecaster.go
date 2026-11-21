package forecast

import (
	"math"
	"time"
)

// Reading is a reading as the forecaster sees it.
type Reading struct {
	At       time.Time
	Celsius  float64
	DeviceID string
}

// Warning is an open excursion warning.
type Warning struct {
	IssuedAt time.Time
	// DeviceID and TemperatureCelsius are those of the reading that issued the warning.
	DeviceID           string
	TemperatureCelsius float64
	Bound              Bound
	BoundCelsius       float64
	// SlopeCelsiusPerMinute is the trend's slope, rounded to three decimals.
	SlopeCelsiusPerMinute float64
	PredictedExcursionAt  time.Time
}

// Decision is what a reading did to the warning of its SSCC.
type Decision int

// Decisions.
const (
	// None: the reading changed no warning.
	None Decision = iota
	// Issue: the reading issued a warning.
	Issue
	// Clear: the reading cleared the open warning.
	Clear
)

// Forecaster runs the warning rule of one SSCC over its readings in device-time order (cold-chain-monitoring.md
// §7.2). It keeps the readings of the trend window and of the operating level, the consecutive readings that point
// at an excursion or clear the open warning, and the open warning.
type Forecaster struct {
	bounds Bounds
	// window holds the readings of the last Window, and level the temperatures of the last LevelReadings in-bounds
	// readings, oldest first.
	window []Point
	level  []float64
	// last is the time of the last reading added; zero before the first.
	last time.Time
	// pointing counts consecutive readings that point at an excursion of the bound pointingAt.
	pointing   int
	pointingAt Bound
	// clearing counts consecutive readings that no longer approach the open warning's bound.
	clearing int
	open     *Warning
}

// New returns a forecaster without readings or warning for a shipment's bounds.
func New(bounds Bounds) *Forecaster {
	return &Forecaster{bounds: bounds}
}

// Seed continues an open warning, as the database records it.
func (f *Forecaster) Seed(open *Warning) {
	if open == nil {
		f.open = nil
		return
	}
	w := *open
	f.open = &w
}

// Open returns the open warning, if any.
func (f *Forecaster) Open() (Warning, bool) {
	if f.open == nil {
		return Warning{}, false
	}
	return *f.open, true
}

// SeedLevel fills the operating level with the temperatures of earlier in-bounds readings, oldest first, as a
// rebuild does before it observes the readings of the window.
func (f *Forecaster) SeedLevel(temperatures []float64) {
	f.level = append(f.level[:0], temperatures[max(0, len(temperatures)-LevelReadings):]...)
}

// Close forgets the open warning, which a breach closed.
func (f *Forecaster) Close() {
	f.open, f.clearing = nil, 0
}

// Observe adds a reading to the window and the operating level without deciding anything, as a rebuild does. A
// reading whose time is not after the last one is ignored, and Observe reports false.
func (f *Forecaster) Observe(r Reading) bool {
	if !f.add(r) {
		return false
	}
	f.pointing, f.clearing = 0, 0
	return true
}

// Step adds a reading and decides whether it issues a warning or clears the open one, which it returns. A reading
// whose time is not after the last one is ignored.
func (f *Forecaster) Step(r Reading) (Decision, Warning) {
	if !f.add(r) {
		return None, Warning{}
	}
	// Out of bounds, the breach rule decides; both counts start again.
	if !f.bounds.Contains(r.Celsius) {
		f.pointing, f.clearing = 0, 0
		return None, Warning{}
	}
	trend, ok := Fit(f.window)
	if !ok {
		f.pointing, f.clearing = 0, 0
		return None, Warning{}
	}
	if f.open == nil {
		return f.considerIssue(r, trend)
	}
	return f.considerClear(trend)
}

// add appends a reading to the window and, if it lies within the bounds, to the level. A gap longer than MaxGap
// empties the window and restarts the counts; the level and the open warning stay.
func (f *Forecaster) add(r Reading) bool {
	if !f.last.IsZero() && !r.At.After(f.last) {
		return false
	}
	if !f.last.IsZero() && r.At.Sub(f.last) > MaxGap {
		f.window = f.window[:0]
		f.pointing, f.clearing = 0, 0
	}
	f.last = r.At
	p := Point{At: r.At, Celsius: r.Celsius}
	f.window = append(trim(f.window, r.At.Add(-Window)), p)
	if f.bounds.Contains(r.Celsius) {
		if len(f.level) == LevelReadings {
			f.level = f.level[1:]
		}
		f.level = append(f.level, r.Celsius)
	}
	return true
}

// trim drops the points before cut. Appending to the result reallocates once its capacity is used up, which copies
// only the points that are left.
func trim(points []Point, cut time.Time) []Point {
	i := 0
	for i < len(points) && points[i].At.Before(cut) {
		i++
	}
	return points[i:]
}

// Level returns the operating level: where the last LevelReadings in-bounds readings cluster most. It is the median
// of the readings within LevelBin of the center of the densest LevelBin-wide bin, so the ramps of excursions, which
// pass through many bins quickly, do not move it, while the readings of a steady reefer, which share a bin or two,
// set it.
func (f *Forecaster) Level() float64 {
	if len(f.level) == 0 {
		return 0
	}
	counts := map[int]int{}
	densest, most := 0, 0
	for _, v := range f.level {
		b := int(math.Floor(v / LevelBin))
		counts[b]++
		if c := counts[b]; c > most || (c == most && b < densest) {
			densest, most = b, c
		}
	}
	center := (float64(densest) + 0.5) * LevelBin
	var near []float64
	for _, v := range f.level {
		if math.Abs(v-center) <= LevelBin {
			near = append(near, v)
		}
	}
	return median(near)
}

// Current returns the current temperature: the median of the readings of the last RecentWindow.
func (f *Forecaster) Current() float64 {
	var values []float64
	cut := f.last.Add(-RecentWindow)
	for _, p := range f.window {
		if !p.At.Before(cut) {
			values = append(values, p.Celsius)
		}
	}
	return median(values)
}

// points reports whether a trend points at an excursion of bound, in the time tau (cold-chain-monitoring.md §7.2).
func (f *Forecaster) points(trend Trend) (Bound, time.Duration, bool) {
	bound, moving := trend.Toward()
	if !moving || math.Abs(trend.Slope) < MinSlope || math.Abs(trend.TStatistic) < MinTStatistic {
		return "", 0, false
	}
	tau, reaches := trend.TimeTo(f.bounds, bound)
	if !reaches || tau > Horizon {
		return "", 0, false
	}
	// The temperature has moved away from the operating level toward the bound. A load that recovers toward its
	// level does not approach the opposite bound, even where a straight line overshoots the level.
	departure := f.Current() - f.Level()
	if bound == Min {
		departure = -departure
	}
	if departure < MinDeparture {
		return "", 0, false
	}
	return bound, tau, true
}

func (f *Forecaster) considerIssue(r Reading, trend Trend) (Decision, Warning) {
	bound, tau, ok := f.points(trend)
	if !ok {
		f.pointing = 0
		return None, Warning{}
	}
	if f.pointing > 0 && f.pointingAt != bound {
		f.pointing = 0
	}
	f.pointing++
	f.pointingAt = bound
	if f.pointing < IssueAfter {
		return None, Warning{}
	}
	f.pointing = 0
	w := Warning{
		IssuedAt: r.At, DeviceID: r.DeviceID, TemperatureCelsius: r.Celsius, Bound: bound,
		BoundCelsius: f.bounds.Value(bound), SlopeCelsiusPerMinute: math.Round(trend.Slope*1000) / 1000,
		PredictedExcursionAt: r.At.Add(tau).Truncate(time.Millisecond),
	}
	f.open = &w
	return Issue, w
}

func (f *Forecaster) considerClear(trend Trend) (Decision, Warning) {
	if tau, reaches := trend.TimeTo(f.bounds, f.open.Bound); reaches && tau <= ClearHorizon {
		f.clearing = 0
		return None, Warning{}
	}
	f.clearing++
	if f.clearing < ClearAfter {
		return None, Warning{}
	}
	cleared := *f.open
	f.open, f.clearing = nil, 0
	return Clear, cleared
}

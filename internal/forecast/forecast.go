// Package forecast forecasts temperature excursions from the trend of a shipment's readings and decides when to
// issue and clear excursion warnings (cold-chain-monitoring.md §7; ADR-0023). It holds no I/O: the detector feeds it
// readings in device-time order and records what it decides.
package forecast

import (
	"math"
	"slices"
	"time"
)

// Forecast rules (cold-chain-monitoring.md §7).
const (
	// Window is how far back the readings of a trend reach.
	Window = 2 * time.Minute
	// MinReadings and MinSpan are what a trend needs at least.
	MinReadings = 12
	MinSpan     = time.Minute
	// MaxGap is the longest silence within a trend: the gap limit of the breach rule.
	MaxGap = 15 * time.Second
	// LevelReadings is how many of the last in-bounds readings the operating level takes: 30 minutes at the
	// nominal interval of 5 s. Counting readings rather than minutes keeps the level of a load that was out of bounds
	// for hours at what the reefer held before.
	LevelReadings = 360
	// LevelBin is the width, in °C, of the bins that find where those readings cluster.
	LevelBin = 0.5
	// RecentWindow is how far back the readings of the current temperature reach.
	RecentWindow = time.Minute
	// MinDeparture is how far, in °C, the current temperature must have moved from the operating level toward a
	// bound.
	MinDeparture = 0.3
	// Horizon is how far ahead a warning looks.
	Horizon = 10 * time.Minute
	// ClearHorizon is the time to excursion beyond which an open warning no longer holds.
	ClearHorizon = 2 * Horizon
	// MinSlope is the slowest trend, in °C per minute, that points at an excursion.
	MinSlope = 0.05
	// MinTStatistic is the significance a trend needs: its slope divided by the slope's standard error.
	MinTStatistic = 3.0
	// IssueAfter and ClearAfter are the consecutive readings that issue and clear a warning.
	IssueAfter = 2
	ClearAfter = 3
)

// Bound names the bound that a temperature approaches.
type Bound string

// Bounds approached.
const (
	// Max is approached by a rising temperature.
	Max Bound = "MAX"
	// Min is approached by a falling temperature.
	Min Bound = "MIN"
)

// Bounds are a shipment's inclusive temperature range.
type Bounds struct {
	Min, Max float64
}

// Contains reports whether t lies within the bounds.
func (b Bounds) Contains(t float64) bool {
	return t >= b.Min && t <= b.Max
}

// Value returns the temperature of a bound.
func (b Bounds) Value(bound Bound) float64 {
	if bound == Max {
		return b.Max
	}
	return b.Min
}

// Point is a reading's time and temperature.
type Point struct {
	At      time.Time
	Celsius float64
}

// Trend is the straight line that ordinary least squares fits to the points of a window.
type Trend struct {
	// At is the time of the last point.
	At       time.Time
	Readings int
	Span     time.Duration
	// Slope is in °C per minute.
	Slope float64
	// TStatistic is the slope divided by its standard error: ±Inf for a perfect fit of a sloped line.
	TStatistic float64
	// Fitted is the temperature of the line at At.
	Fitted float64
}

// Fit fits a line to points, oldest first. It reports false with fewer than MinReadings points or a span shorter
// than MinSpan.
func Fit(points []Point) (Trend, bool) {
	n := len(points)
	if n < MinReadings {
		return Trend{}, false
	}
	last := points[n-1].At
	span := last.Sub(points[0].At)
	if span < MinSpan {
		return Trend{}, false
	}
	// Times are minutes before the last point, which keeps the sums small.
	var meanX, meanY float64
	for _, p := range points {
		meanX += p.At.Sub(last).Minutes()
		meanY += p.Celsius
	}
	meanX /= float64(n)
	meanY /= float64(n)
	var sxx, sxy float64
	for _, p := range points {
		dx := p.At.Sub(last).Minutes() - meanX
		sxx += dx * dx
		sxy += dx * (p.Celsius - meanY)
	}
	slope := sxy / sxx
	intercept := meanY - slope*meanX
	var ssr float64
	for _, p := range points {
		r := p.Celsius - intercept - slope*p.At.Sub(last).Minutes()
		ssr += r * r
	}
	se := math.Sqrt(ssr / float64(n-2) / sxx)
	var tstat float64
	switch {
	case se > 0:
		tstat = slope / se
	case slope != 0:
		tstat = math.Copysign(math.Inf(1), slope)
	}
	return Trend{At: last, Readings: n, Span: span, Slope: slope, TStatistic: tstat, Fitted: intercept}, true
}

// Toward returns the bound that the trend approaches, if it moves at all.
func (t Trend) Toward() (Bound, bool) {
	switch {
	case t.Slope > 0:
		return Max, true
	case t.Slope < 0:
		return Min, true
	}
	return "", false
}

// TimeTo returns how long the trend takes to reach the bound, 0 if the fitted temperature is already past it, and
// false if the trend does not move toward it.
func (t Trend) TimeTo(b Bounds, bound Bound) (time.Duration, bool) {
	limit := b.Value(bound)
	var distance, speed float64 // °C, and °C per minute toward the bound
	if bound == Max {
		distance, speed = limit-t.Fitted, t.Slope
	} else {
		distance, speed = t.Fitted-limit, -t.Slope
	}
	if speed <= 0 {
		return 0, false
	}
	if distance <= 0 {
		return 0, true
	}
	minutes := distance / speed
	if minutes > ClearHorizon.Minutes()*1000 {
		// Far beyond any horizon; keeps the conversion within range.
		minutes = ClearHorizon.Minutes() * 1000
	}
	return time.Duration(minutes * float64(time.Minute)), true
}

// Median returns the median of values, which it reorders. It returns 0 for none.
func median(values []float64) float64 {
	n := len(values)
	if n == 0 {
		return 0
	}
	slices.Sort(values)
	if n%2 == 1 {
		return values[n/2]
	}
	return (values[n/2-1] + values[n/2]) / 2
}

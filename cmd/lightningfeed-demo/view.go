package main

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/mheers/nats-lightning/internal/geo"
)

// The map is a fixed-size character grid rather than a scrolling log, because
// the interesting property of a lightning feed is not any single stroke but
// where they cluster: a storm crossing a region reads as a moving line, and no
// list of timestamps shows that.
const (
	// mapRows and mapCols give a window twice as wide as it is tall, which is the
	// aspect a character cell already has. Drawing a circle of equal width and
	// height in characters without this correction produces a visibly squashed
	// ellipse, and a squashed ellipse misrepresents the only geometry on screen.
	mapRows = 22
	mapCols = 44

	// mapPad is the margin of empty space around the region, as a multiple of the
	// radius. Strokes in the cells overlapping the region but beyond the radius
	// are still published, so a window that stopped at the boundary would hide
	// exactly the strokes a consumer most wants to see arrive and be discarded.
	mapPad = 1.2

	// mapFallbackCols is used when the terminal width cannot be determined.
	mapFallbackCols = 80
)

// kmPerDegLat is the length of a degree of latitude.
//
// It is duplicated from the geo package rather than exported from there because
// this is presentation, not classification: nothing here decides whether a
// stroke is inside the region, and a projection is allowed to be approximate.
const kmPerDegLat = 111.32

// projection is a flat window onto the map.
//
// It is an equirectangular projection, which is wrong by construction: the
// meridians are drawn parallel rather than converging, so distances near the
// edges of a wide window are exaggerated. That is an acceptable price here
// because the region is a small circle and the strokes are points, and the
// alternative — a proper projection — buys nothing at 44 columns wide.
type projection struct {
	lat, lon float64 // window centre, degrees
	latSpan  float64 // north to south, degrees
	lonSpan  float64 // east to west, degrees
	rows     int
	cols     int
	region   geo.Circle
}

// newProjection fits a window around the region, or around the whole world when
// no region is configured.
func newProjection(region geo.Circle, cols int) projection {
	p := projection{rows: mapRows, cols: cols, region: region}

	if region.RadiusKm <= 0 {
		// World-wide. The poles are excluded because no stroke is ever reported
		// there and the top and bottom rows would otherwise be permanently empty.
		p.lat, p.lon = 0, 0
		p.latSpan, p.lonSpan = 170, 360
		return p
	}

	p.lat, p.lon = region.Lat, region.Lon

	// The window is square in kilometres, which is what makes a circle look like a
	// circle. A degree of longitude is shorter than a degree of latitude away from
	// the equator, so the longitude span is widened by exactly the factor that
	// keeps the two spans the same length on the ground.
	spanKm := 2 * region.RadiusKm * mapPad
	p.latSpan = spanKm / kmPerDegLat

	// The cosine is clamped because it reaches zero at the poles, where the
	// division below would produce an infinite longitude span and a map that
	// silently covers half the planet.
	cosLat := math.Max(0.05, math.Cos(region.Lat*math.Pi/180))
	p.lonSpan = spanKm / (kmPerDegLat * cosLat)
	return p
}

// position returns the coordinates at the centre of one grid cell.
func (p projection) position(col, row int) (lat, lon float64) {
	lon = p.lon - p.lonSpan/2 + p.lonSpan*float64(col)/p.den(p.cols)
	lat = p.lat + p.latSpan/2 - p.latSpan*float64(row)/p.den(p.rows)
	return lat, lon
}

// den is a division that cannot divide by zero.
//
// A one-cell axis is degenerate but still has to produce a finite coordinate;
// returning 1 puts every cell at the centre rather than producing NaN, which
// would print as a map of question marks.
func (p projection) den(n int) float64 {
	if n <= 1 {
		return 1
	}
	return float64(n - 1)
}

// project maps coordinates onto a grid cell, reporting whether they fall inside
// the window at all.
func (p projection) project(lat, lon float64) (col, row int, ok bool) {
	north, west := p.lat+p.latSpan/2, p.lon-p.lonSpan/2

	// Normalised position within the window, 0..1 on each axis.
	fx := (lon - west) / p.lonSpan
	fy := (north - lat) / p.latSpan
	if fx < 0 || fx > 1 || fy < 0 || fy > 1 {
		return 0, 0, false
	}

	col = int(math.Round(fx * float64(p.cols-1)))
	row = int(math.Round(fy * float64(p.rows-1)))
	return clamp(col, 0, p.cols-1), clamp(row, 0, p.rows-1), true
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// marker is one stroke held for the map until it fades.
type marker struct {
	lat, lon float64
	at       time.Time
}

// faded marks the glyph for a marker of the given age.
//
// The three levels are not decoration: a stroke cluster that is minutes old is
// history and one that is seconds old is the storm happening now, and a map
// that draws both the same way cannot tell an active cell from a dead one.
func faded(age, fade time.Duration) rune {
	switch {
	case fade <= 0:
		return '*'
	case age < fade/6:
		return '*'
	case age <= fade/2:
		return '+'
	default:
		return ':'
	}
}

// renderMap draws the map, the strokes and a legend.
func renderMap(p projection, markers []marker, now time.Time, fade time.Duration) []string {
	canvas := p.canvas()

	// The region centre goes down before the strokes so that a stroke landing on
	// it wins: an actual discharge is more useful information than the fact that
	// the centre is where it is, which the header line already says.
	if p.region.RadiusKm > 0 {
		if col, row, ok := p.project(p.region.Lat, p.region.Lon); ok {
			canvas[row][col] = '@'
		}
	}

	// Newest wins per cell, decided by age rather than by iteration order.
	//
	// Markers arrive in arrival order, which is not the same as being sorted, and
	// a single stroke landing in a cell several others just left should be drawn
	// on top of them. Comparing the ages explicitly is what makes that hold
	// whatever order the caller hands them over in.
	age := make([][]float64, p.rows)
	for row := range age {
		age[row] = make([]float64, p.cols)
		for col := range age[row] {
			age[row][col] = math.Inf(1)
		}
	}

	for _, m := range markers {
		col, row, ok := p.project(m.lat, m.lon)
		if !ok {
			continue
		}
		if seconds := now.Sub(m.at).Seconds(); seconds < age[row][col] {
			age[row][col] = seconds
			canvas[row][col] = faded(time.Duration(seconds*float64(time.Second)), fade)
		}
	}

	lines := make([]string, 0, p.rows+2)
	for _, row := range canvas {
		lines = append(lines, "  "+strings.TrimRight(string(row), " "))
	}
	lines = append(lines, "  * now   + recent   : fading   · inside radius   @ region centre")
	return lines
}

// canvas paints the region onto an empty grid.
//
// Membership is tested with the same exact circle test the bridge uses, on each
// cell's centre, so what is drawn is the region itself rather than an
// approximation of it. The cost is rows×cols haversine calls on every redraw,
// which is about a thousand of them a second — nothing next to subscribing.
func (p projection) canvas() [][]rune {
	canvas := make([][]rune, p.rows)
	for row := range canvas {
		canvas[row] = []rune(strings.Repeat(" ", p.cols))
		if p.region.RadiusKm <= 0 {
			continue
		}
		for col := range canvas[row] {
			lat, lon := p.position(col, row)
			if p.region.ContainsPoint(lat, lon) {
				canvas[row][col] = '·'
			}
		}
	}
	return canvas
}

// sparkline renders per-bucket counts as a row of block glyphs.
//
// Unicode block elements are used rather than digits because the row has to fit
// under the map without wrapping, and a run of digits at one character per
// bucket would wrap on any narrow terminal.
func sparkline(buckets []int) string {
	if len(buckets) == 0 {
		return ""
	}
	blocks := []rune{'▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'}

	peak := 0
	for _, n := range buckets {
		if n > peak {
			peak = n
		}
	}

	var b strings.Builder
	for _, n := range buckets {
		switch {
		case n <= 0:
			b.WriteRune(blocks[0])
		case peak <= 1:
			// Every bucket holds the peak, or the peak is the only non-zero one.
			// The general formula divides by peak-1 and would divide by zero here.
			b.WriteRune(blocks[len(blocks)-1])
		default:
			// Scaled so the busiest bucket is always full, with the bottom block
			// reserved for a non-zero value, so a single stroke is visible rather
			// than rounding down to nothing.
			idx := 1 + (n-1)*(len(blocks)-2)/(peak-1)
			b.WriteRune(blocks[clamp(idx, 1, len(blocks)-1)])
		}
	}
	return b.String()
}

// formatAge renders a duration the way the status line wants it: coarse, and
// never a decimal, because it is read at a glance while the map is redrawing.
func formatAge(d time.Duration) string {
	switch {
	case d < time.Second:
		return "now"
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
}

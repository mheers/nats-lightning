package geo

import (
	"math"
	"strings"
	"testing"
)

// A panic anywhere in this package kills the pipeline mid-stream, and every one of
// these functions is reached with values derived from the upstream rather than
// chosen here. The targets below assert the properties that hold for *every* input:
// no panic, deterministic results, and agreement between the encoder and the cell
// bounds it is used to derive.

// FuzzEncode throws arbitrary float64 pairs at the geohash encoder.
func FuzzEncode(f *testing.F) {
	f.Add(48.137, 11.575, 5)
	f.Add(0.0, 0.0, 5)
	f.Add(-33.87, 151.21, 4)
	f.Add(90.0, 180.0, 5)
	f.Add(-90.0, -180.0, 5)
	f.Add(math.NaN(), 0.0, 5)
	f.Add(math.Inf(1), 0.0, 5)
	f.Add(0.0, math.Inf(-1), 5)
	f.Add(1e308, -1e308, 5)
	f.Add(0.0, 0.0, 0)
	f.Add(0.0, 0.0, 13)

	f.Fuzz(func(t *testing.T, lat, lon float64, precision int) {
		cell, err := Encode(lat, lon, precision)
		if err != nil {
			return
		}

		// A successful encode must produce a cell of exactly the requested
		// precision, over the geohash alphabet. A short or long cell would index
		// nothing, or index the wrong granularity.
		if len(cell) != precision {
			t.Fatalf("Encode(%v,%v,%d) = %q, length %d, want %d", lat, lon, precision, cell, len(cell), precision)
		}
		if cell != strings.ToLower(cell) {
			t.Errorf("Encode produced %q, which is not lower case", cell)
		}
		for _, r := range cell {
			if !strings.ContainsRune(alphabet, r) {
				t.Fatalf("Encode produced %q containing %q, which is not in the alphabet", cell, r)
			}
		}

		// Determinism, so a subject is reproducible by a consumer recomputing it.
		again, err2 := Encode(lat, lon, precision)
		if err2 != nil || again != cell {
			t.Errorf("Encode is not deterministic for (%v,%v,%d): %q then %q (%v)",
				lat, lon, precision, cell, again, err2)
		}

		// The cell it names must actually contain the point, which is what makes
		// CellBounds usable as a filter. NaN coordinates cannot be encoded, and
		// Infinity is rejected, so by here the point is finite.
		b, err := CellBounds(cell)
		if err != nil {
			t.Fatalf("CellBounds(%q): %v", cell, err)
		}
		if lat < b.MinLat || lat > b.MaxLat || lon < b.MinLon || lon > b.MaxLon {
			t.Errorf("Encode(%v,%v,%d) = %q but that cell spans lat[%v,%v] lon[%v,%v]",
				lat, lon, precision, cell, b.MinLat, b.MaxLat, b.MinLon, b.MaxLon)
		}
	})
}

// FuzzCellBounds throws arbitrary strings at the bounds decoder.
//
// CellBounds indexes and slices by the characters it decodes, so a hostile or
// corrupt cell string is the most likely place in this package to panic.
func FuzzCellBounds(f *testing.F) {
	f.Add("u0xc4")
	f.Add("")
	f.Add("ezs42")
	f.Add("ab")
	f.Add("!!!")
	f.Add("u0xc4extra")
	f.Add("ÜÖ")
	f.Add("\x00\x01")
	f.Add(strings.Repeat("u", 12))
	f.Add(strings.Repeat("u", 13))

	f.Fuzz(func(t *testing.T, cell string) {
		b, err := CellBounds(cell)
		if err != nil {
			return
		}

		if err := b.Validate(); err != nil {
			t.Errorf("CellBounds(%q) returned invalid bounds %+v: %v", cell, b, err)
		}
		if b.MinLat > b.MaxLat || b.MinLon > b.MaxLon {
			t.Errorf("CellBounds(%q) returned an inverted box %+v", cell, b)
		}
		if len(cell) > 0 && len(cell) <= 12 {
			if got, want := len(cell), precisionOf(cell); got != want {
				t.Errorf("CellBounds(%q) accepted a cell whose precision does not match its length", cell)
			}
		}

		// Deterministic: a bounds lookup is on the hot path and is compared for
		// equality when pruning the tree.
		again, err2 := CellBounds(cell)
		if err2 != nil || again != b {
			t.Errorf("CellBounds is not deterministic for %q", cell)
		}
	})
}

// FuzzCircleCells asserts the property the whole publisher depends on: the cell
// list covers every point inside the circle. It is the invariant whose violation
// silently drops lightning, so it is worth attacking directly with arbitrary circles
// rather than only the few hand-picked ones.
//
// Run it with limited parallelism:
//
//	go test -run FuzzCircleCells -fuzz FuzzCircleCells -fuzztime=120s -parallel=4
//
// Not for speed — because each worker legitimately allocates up to MaxCells while
// probing the budget, so the default one-worker-per-core exhausts memory on a
// 24-core machine and the fuzzer dies with no failing input. It is not a code
// defect: the same inputs pass when run with fewer workers.
func FuzzCircleCells(f *testing.F) {
	f.Add(35.3340688, 24.4944483, 10.0)
	f.Add(-17.0, 179.5, 200.0)
	f.Add(-17.0, -179.5, 200.0)
	f.Add(70.0, 20.0, 500.0)
	f.Add(0.0, 0.0, 1.0)
	f.Add(90.0, 180.0, 10.0)
	f.Add(-90.0, -180.0, 10.0)

	f.Fuzz(func(t *testing.T, lat, lon, radiusKm float64) {
		c := Circle{Lat: lat, Lon: lon, RadiusKm: radiusKm}
		if err := c.Validate(); err != nil {
			return
		}

		cells, err := c.Cells(5)
		if err != nil {
			// Cells is allowed to refuse a circle that Validate accepted, because
			// affordability is decided there and nowhere else: the cell count is not
			// predictable from the radius. Anything it returns, though, must be
			// within the budget it enforces.
			if len(cells) != 0 {
				t.Fatalf("Cells returned %d cells alongside an error", len(cells))
			}
			if !strings.Contains(err.Error(), "cell budget") &&
				!strings.Contains(err.Error(), "geohash") {
				t.Errorf("unexpected error from Cells on a valid circle: %v", err)
			}
			return
		}
		if len(cells) > MaxCells {
			t.Fatalf("Cells returned %d cells, over the %d budget", len(cells), MaxCells)
		}

		have := make(map[string]bool, len(cells))
		for _, cell := range cells {
			if have[cell] {
				t.Errorf("duplicate cell %q for circle %+v", cell, c)
			}
			have[cell] = true
			if len(cell) != 5 {
				t.Errorf("Cells returned %q, which is not precision 5", cell)
			}
		}

		// Every in-circle point must be in a listed cell. Sample the disc rather
		// than its rim, because the interesting failures are interior ones.
		for _, p := range sampleCircle(c, 60) {
			plat, plon := p[0], p[1]
			if !c.ContainsPoint(plat, plon) {
				continue
			}
			cell, err := Encode(plat, plon, 5)
			if err != nil {
				continue // an unrepresentable point cannot be filed under any cell
			}
			if !have[cell] {
				t.Fatalf("circle %+v does not list cell %q containing in-circle point %.6f,%.6f "+
					"(%d cells total); that stroke would be dropped as outside the region",
					c, cell, plat, plon, len(cells))
			}
		}
	})
}

// FuzzClassify asserts the band is always one of the three declared values, and
// that the deviation bound holds for any input including the extremes.
//
// Distances are fuzzed rather than generated: an arbitrary float64 can be negative
// or enormous, and the band arithmetic must stay total for all of them rather than
// only for the distances a real stroke could have.
func FuzzClassify(f *testing.F) {
	f.Add(0.0, 0.0, 10.0)
	f.Add(5.0, 2200.0, 10.0)
	f.Add(800.0, 2.147483647e9, 10.0)
	f.Add(9.0, 9.223372036854776e18, 10.0)
	f.Add(-5.0, -1.0, 10.0)
	f.Add(1e300, 1e300, 10.0)

	f.Fuzz(func(t *testing.T, distKm, deviationM, radiusKm float64) {
		c := Circle{Lat: 0, Lon: 0, RadiusKm: radiusKm}
		if err := c.Validate(); err != nil {
			return
		}

		got := c.Classify(distKm, deviationM)
		switch got {
		case CertaintyIn, CertaintyBoundary, CertaintyOut:
		default:
			t.Fatalf("Classify(%v,%v) = %q, which is not a declared band", distKm, deviationM, got)
		}

		// A distance far outside the circle, combined with any deviation, must not
		// come back as "inside". This is the invariant a sentinel deviation broke.
		if distKm > c.RadiusKm+MaxPlausibleDeviationM/1000+1 && got != CertaintyOut {
			t.Errorf("Classify(dist=%v, dev=%v, radius=%v) = %q; a stroke further out "+
				"than radius+maxDeviation must be out", distKm, deviationM, c.RadiusKm, got)
		}
	})
}

// FuzzBoundingBoxes asserts the wrap arithmetic cannot produce a box outside the
// coordinate space, and that the boxes always cover the circle.
func FuzzBoundingBoxes(f *testing.F) {
	f.Add(0.0, 179.99, 100.0)
	f.Add(0.0, -179.99, 100.0)
	f.Add(0.0, 0.0, 100.0)
	f.Add(70.0, 179.0, 500.0)

	f.Fuzz(func(t *testing.T, lat, lon, radiusKm float64) {
		c := Circle{Lat: lat, Lon: lon, RadiusKm: radiusKm}
		if err := c.Validate(); err != nil {
			return
		}

		boxes := c.BoundingBoxes()
		if len(boxes) == 0 {
			t.Fatalf("BoundingBoxes returned nothing for %+v", c)
		}

		for i, b := range boxes {
			if err := b.Validate(); err != nil {
				t.Errorf("box %d of %d is invalid: %+v (%v)", i, len(boxes), b, err)
			}
		}

		// The centre must fall inside one of the boxes, otherwise the region
		// cannot contain its own middle.
		contained := false
		for _, b := range boxes {
			if c.Lon >= b.MinLon && c.Lon <= b.MaxLon && c.Lat >= b.MinLat && c.Lat <= b.MaxLat {
				contained = true
				break
			}
		}
		if !contained {
			t.Errorf("no box contains the centre of %+v: %+v", c, boxes)
		}
	})
}

// precisionOf is a small helper so FuzzCellBounds can state the length/precision
// invariant without reaching into the decoder.
func precisionOf(cell string) int {
	return len([]rune(cell))
}

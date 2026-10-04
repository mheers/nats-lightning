package geo

import (
	"math"
	"testing"
)

// A circle near the antimeridian used to lose the half of itself on the far side.
//
// BoundingBox clamped MaxLon to 180, Cells walked the tree against that clamped box,
// and so the cells east of the seam were never enumerated. Everywhere else this
// package errs generous — cell selection is a deliberate superset — so this was the
// one place where "safe direction" quietly inverted. The consequences were both
// silent: ingest dropped the strokes as outside_region, blaming the region for
// dropping strokes that were inside it, and the radius query could not find them
// either. Measured on a 200 km circle at longitude 179.5: 28% of in-circle points
// fell in cells the region did not list.

// antimeridianRegions covers both wrap directions and a normal region as a control.
func antimeridianRegions() []Circle {
	return []Circle{
		{Lat: -17.0, Lon: 179.5, RadiusKm: 200},  // wraps east
		{Lat: -17.0, Lon: -179.5, RadiusKm: 200}, // wraps west
		{Lat: 0.0, Lon: 179.9, RadiusKm: 50},
		{Lat: 64.0, Lon: -179.8, RadiusKm: 300}, // high latitude, wide longitude span
		// Far from the seam, but poleward: the bounding box used to measure its
		// longitude half-width at the centre rather than at the narrowest degree in
		// the span, so it under-covered the poleward side and lost 6 cells' worth of
		// the circle's own area.
		{Lat: 70.0, Lon: 20.0, RadiusKm: 500},
		{Lat: -70.0, Lon: 20.0, RadiusKm: 500},
	}
}

func TestCellsCoverTheWholeCircleAcrossTheAntimeridian(t *testing.T) {
	for _, c := range antimeridianRegions() {
		t.Run(describeCircle(c), func(t *testing.T) {
			cells, err := c.Cells(5)
			if err != nil {
				t.Fatalf("Cells: %v", err)
			}
			have := make(map[string]bool, len(cells))
			for _, cell := range cells {
				have[cell] = true
			}

			missing, checked := 0, 0
			for _, p := range sampleCircle(c, 3000) {
				lat, lon := p[0], p[1]
				// Only points inside the circle matter. The cell list is a superset
				// of the circle's bounding box, not of the plane, so a point outside
				// the circle may legitimately have an unlisted cell.
				if !c.ContainsPoint(lat, lon) {
					continue
				}
				checked++
				cell, err := Encode(lat, lon, 5)
				if err != nil {
					t.Fatalf("Encode(%v,%v): %v", lat, lon, err)
				}
				if !have[cell] {
					if missing == 0 {
						t.Errorf("first missing cell %s for in-circle point %.4f,%.4f",
							cell, lat, lon)
					}
					missing++
				}
			}
			if checked < 1000 {
				t.Fatalf("only %d samples landed inside the circle; this test would "+
					"pass without proving much", checked)
			}
			if missing > 0 {
				t.Errorf("%d of %d in-circle points are in unlisted cells, so those "+
					"strokes would be dropped as outside_region", missing, checked)
			}
		})
	}
}

// TestCellsAcrossTheAntimeridianAreDeduplicated guards the union in Cells: the two
// boxes are disjoint, but a cell could still be appended twice if the walks ever
// overlapped, and a duplicate would inflate the publisher's membership set and the
// store's IN list for no reason.
func TestCellsAcrossTheAntimeridianAreDeduplicated(t *testing.T) {
	for _, c := range antimeridianRegions() {
		t.Run(describeCircle(c), func(t *testing.T) {
			cells, err := c.Cells(5)
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[string]bool, len(cells))
			for _, cell := range cells {
				if seen[cell] {
					t.Errorf("cell %s appears twice in %d cells", cell, len(cells))
				}
				seen[cell] = true
			}
		})
	}
}

// TestBoundingBoxStaysContiguousForTheViewport pins the split: the viewport hint
// wants one unwrapped box, because a subscriber selecting across the seam is one
// contiguous request. BoundingBoxes is for cell enumeration, where the wrap must be
// honoured.
func TestBoundingBoxStaysContiguousForTheViewport(t *testing.T) {
	c := Circle{Lat: -17.0, Lon: 179.5, RadiusKm: 200}

	if got := len(c.BoundingBoxes()); got != 2 {
		t.Errorf("BoundingBoxes returned %d boxes, want 2 for a region crossing the seam", got)
	}
	b := c.BoundingBox()
	if b.MaxLon > 180 || b.MinLon < -180 {
		t.Errorf("BoundingBox %+v escaped the coordinate space", b)
	}

	// A region nowhere near the seam is still one box.
	normal := Circle{Lat: 35.3340688, Lon: 24.4944483, RadiusKm: 10}
	if got := len(normal.BoundingBoxes()); got != 1 {
		t.Errorf("BoundingBoxes returned %d boxes for an ordinary region, want 1", got)
	}
}

func TestCellsStillRejectsAnInvalidCircle(t *testing.T) {
	for _, c := range []Circle{
		{Lat: 0, Lon: 0, RadiusKm: 0},
		{Lat: 0, Lon: 0, RadiusKm: -1},
		{Lat: 91, Lon: 0, RadiusKm: 10},
		{Lat: 0, Lon: 181, RadiusKm: 10},
	} {
		if _, err := c.Cells(5); err == nil {
			t.Errorf("Cells accepted %+v", c)
		}
	}
}

// MaxRadiusKm exists because cell enumeration is O(area) and the result is held in
// memory, again in the region table as JSON, and re-parsed by json_each on every
// query. Measured: 1000 km is 251k cells and 15 MiB, 10000 km is 18.3M cells and
// 950 MiB, and 20000 km is the whole world. Nothing bounded the flag.
func TestRadiusIsBounded(t *testing.T) {
	if _, err := (Circle{Lat: 0, Lon: 0, RadiusKm: MaxRadiusKm + 1}).Cells(5); err == nil {
		t.Errorf("a %v km radius was accepted", MaxRadiusKm+1)
	}
	// The boundary itself must still work.
	if err := (Circle{Lat: 0, Lon: 0, RadiusKm: MaxRadiusKm}).Validate(); err != nil {
		t.Errorf("the documented maximum %v km was rejected: %v", MaxRadiusKm, err)
	}
	// And an ordinary region must be unaffected.
	if err := (Circle{Lat: 35.3340688, Lon: 24.4944483, RadiusKm: 10}).Validate(); err != nil {
		t.Errorf("a 10 km region was rejected: %v", err)
	}
}

// A deviation of INT_MAX — the commonest "no data" sentinel in this family of
// protocols — made every stroke within radius+dev classify as boundary, which the
// default policy accepts. One such stroke therefore published every stroke on earth,
// with no counter moved and the region still reported as configured. The negative
// side was already clamped; the positive side was unbounded.
func TestAnAbsurdDeviationCannotWidenTheRegion(t *testing.T) {
	const radiusKm = 10.0
	c := Circle{Lat: 0, Lon: 0, RadiusKm: radiusKm}
	const distKm = 800 // far outside

	// Every plausible deviation agrees the stroke is out.
	for _, dev := range []float64{0, 2.2, 15, 300, MaxPlausibleDeviationM} {
		if got := c.Classify(distKm, dev); got != CertaintyOut {
			t.Errorf("Classify(800km, dev=%vm) = %q, want %q", dev, got, CertaintyOut)
		}
	}

	// The sentinels that used to break it.
	for _, dev := range []float64{999_999, 1 << 20, math.MaxInt32, math.MaxInt64} {
		got := c.Classify(distKm, dev)
		if got == CertaintyBoundary {
			t.Errorf("Classify(800km, dev=%v) = %q, which the default policy accepts; "+
				"a %v km radius region was widened to %v km by one field",
				dev, got, radiusKm, radiusKm+dev/1000)
		}
	}

	// The negative side keeps working.
	if got := c.Classify(distKm, -1); got != CertaintyOut {
		t.Errorf("Classify(800km, dev=-1) = %q, want %q", got, CertaintyOut)
	}
}

// The bound must not move the bands that matter, or it is not a bound but a change
// of policy.
func TestTheDeviationBoundLeavesRealBandsAlone(t *testing.T) {
	c := Circle{Lat: 0, Lon: 0, RadiusKm: 10}

	tests := []struct {
		name   string
		distKm float64
		devM   float64
		want   Certainty
	}{
		{"certainly inside", 5, 2000, CertaintyIn},
		{"certainly outside", 50, 2000, CertaintyOut},
		{"undecidable", 9, 5000, CertaintyBoundary},
		// Exactly on the in/out line: dist+dev == radius counts as inside.
		{"exactly on the boundary, in", 9.999, 1, CertaintyIn},
		// A deviation at exactly the plausible maximum behaves like any large one:
		// undecidable rather than certainly inside.
		{"at the bound", 9, MaxPlausibleDeviationM, CertaintyBoundary},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.Classify(tc.distKm, tc.devM); got != tc.want {
				t.Errorf("Classify(%v, %v) = %q, want %q", tc.distKm, tc.devM, got, tc.want)
			}
		})
	}
}

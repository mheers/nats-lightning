package geo

import (
	"fmt"
	"math"
	"testing"
)

// Slice 5: Circle.Cells selects the geohash cells that can contain a stroke
// inside the circle.
//
// The central requirement is one-directional and must never fail: no point
// inside the circle may fall outside the returned cell set. A missing cell does
// not error, it silently drops lightning, so the coverage property gets the
// most attention here. Being slightly too generous only costs a little wasted
// bandwidth, which is recoverable.

func TestCellsCoversEveryPointInsideTheConfiguredRegion(t *testing.T) {
	for _, c := range []Circle{
		{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad},
		{Lat: 68, Lon: 25, RadiusKm: 50},             // high latitude, wide
		{Lat: -33, Lon: 151, RadiusKm: 25},           // southern hemisphere
		{Lat: 0, Lon: 0, RadiusKm: 200},              // spans the equator
		{Lat: 35.3340688, Lon: -179.9, RadiusKm: 30}, // near the dateline
	} {
		t.Run(fmt.Sprintf("lat=%.4f_lon=%.4f_r=%.0f", c.Lat, c.Lon, c.RadiusKm), func(t *testing.T) {
			const precision = 5

			cells, err := c.Cells(precision)
			if err != nil {
				t.Fatalf("Cells(%d): %v", precision, err)
			}
			set := ToSet(cells)
			if set == nil {
				t.Fatal("Cells returned an empty set for a valid circle")
			}

			// Sample the disc on a spiral: uniform angle, sqrt radial spacing
			// gives uniform area density, and the golden angle avoids banding.
			const samples = 20000
			for i := range samples {
				angle := float64(i) * 2.399963229728653
				rad := c.RadiusKm * math.Sqrt(float64(i+1)/samples)

				lat := c.Lat + rad*math.Cos(angle)/kmPerDegLat
				lon := c.Lon + rad*math.Sin(angle)/(kmPerDegLat*cosLatKm(c.Lat))

				if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
					continue
				}
				if !c.ContainsPoint(lat, lon) {
					continue // float drift at the very edge
				}

				cell, err := Encode(lat, lon, precision)
				if err != nil {
					t.Fatalf("Encode(%v, %v): %v", lat, lon, err)
				}
				if _, ok := set[cell]; !ok {
					t.Fatalf("point (%v, %v) lies inside the circle but its cell %q "+
						"is absent from the %d returned cells", lat, lon, cell, len(cells))
				}
			}
		})
	}
}

func TestCellsReturnsTheExpectedCountForTheConfiguredRegion(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}

	// Precision 5 was chosen for exactly this reason: 30 subjects covers a
	// 10 km circle with about 1.9x overshoot. Precision 4 needs only 2
	// subjects but passes ~4x the circle's worth of strokes; precision 6 would
	// need 693 subjects, which is absurd for one village.
	tests := []struct {
		precision int
		want      int
	}{
		{4, 2},
		{5, 30},
	}
	for _, tc := range tests {
		cells, err := c.Cells(tc.precision)
		if err != nil {
			t.Fatalf("Cells(%d): %v", tc.precision, err)
		}
		if len(cells) != tc.want {
			t.Errorf("Cells(%d) returned %d cells, want %d", tc.precision, len(cells), tc.want)
		}
	}
}

func TestCellsDoesNotReturnCellsFarFromTheRegion(t *testing.T) {
	// A buggy tree walk that failed to prune would return cells on the far side
	// of the planet. Each returned cell must be plausibly near the circle.
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}

	cells, err := c.Cells(5)
	if err != nil {
		t.Fatalf("Cells: %v", err)
	}
	for _, cell := range cells {
		lat, lon, err := CellCenter(cell)
		if err != nil {
			t.Fatalf("CellCenter(%q): %v", cell, err)
		}
		// A cell centre can legitimately sit up to half a cell diagonal past
		// the radius, so allow generous slack before calling it a straggler.
		if d := HaversineKm(c.Lat, c.Lon, lat, lon); d > c.RadiusKm+10 {
			t.Errorf("cell %q centre is %.1f km away, far outside a %.0f km region",
				cell, d, c.RadiusKm)
		}
	}
}

// Coarser precision must never return fewer cells than finer precision, or a
// coarser configuration would drop strokes the finer one would have caught.
func TestCellsIsMonotonicInPrecision(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}

	prev := 0
	for precision := 1; precision <= 6; precision++ {
		cells, err := c.Cells(precision)
		if err != nil {
			t.Fatalf("Cells(%d): %v", precision, err)
		}
		if len(cells) < prev {
			t.Errorf("Cells(%d) returned %d cells, fewer than the %d at precision %d",
				precision, len(cells), prev, precision-1)
		}
		prev = len(cells)
	}
}

func TestCellsRejectsAnUnusableCircle(t *testing.T) {
	if _, err := (Circle{Lat: rousLat, Lon: rousLon, RadiusKm: 0}).Cells(5); err == nil {
		t.Error("Cells on a zero-radius circle = nil error, want error")
	}
	if _, err := (Circle{Lat: 0, Lon: 0, RadiusKm: 10}).Cells(0); err == nil {
		t.Error("Cells at precision 0 = nil error, want error")
	}
	if _, err := (Circle{Lat: 0, Lon: 0, RadiusKm: 10}).Cells(13); err == nil {
		t.Error("Cells at precision 13 = nil error, want error")
	}
}

func TestToSetBuildsAMembershipSet(t *testing.T) {
	set := ToSet([]string{"sw33j", "sw33k", "sw33m"})
	if len(set) != 3 {
		t.Fatalf("len = %d, want 3", len(set))
	}
	if !InCells(set, "sw33j") {
		t.Error("expected sw33j to be a member")
	}
	if InCells(set, "sw32d") {
		t.Error("did not expect sw32d to be a member")
	}
}

// An empty set means "no cell restriction", which is what an unconfigured
// world-wide deployment uses. It must let everything through rather than
// filtering everything out.
func TestEmptyCellSetImposesNoRestriction(t *testing.T) {
	if !InCells(ToSet(nil), "anything") {
		t.Error("an empty cell set must not filter anything out")
	}
	if !InCells(nil, "anything") {
		t.Error("a nil cell set must not filter anything out")
	}
	if ToSet(nil) != nil {
		t.Error("ToSet(nil) should return nil, not an empty map")
	}
}

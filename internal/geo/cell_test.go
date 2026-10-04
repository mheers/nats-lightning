package geo

import (
	"math"
	"testing"
)

// Slice 2: CellBounds recovers the geographic extent of a geohash.
//
// The geohash standard interleaves longitude and latitude bits globally across
// the whole string. A cell of precision p therefore carries 5p bits, of which
// ceil(5p/2) refine longitude and floor(5p/2) refine latitude. The tests below
// assert against that closed form, which comes from the standard rather than
// from this package's implementation.

// wantSpans returns the expected longitude and latitude span, in degrees, of a
// cell at the given precision.
func wantSpans(precision int) (lonSpan, latSpan float64) {
	bits := 5 * precision
	lonBits := (bits + 1) / 2 // ceil(bits/2)
	latBits := bits / 2       // floor(bits/2)
	return 360 / math.Ldexp(1, lonBits), 180 / math.Ldexp(1, latBits)
}

func TestCellBoundsMatchTheStandardSpans(t *testing.T) {
	// At precision 1 the alphabet's 32 characters divide the world into
	// 8 longitude bands by 4 latitude bands, so each cell spans 45 degrees.
	// Sanity-check the closed form itself before relying on it.
	if lon, lat := wantSpans(1); lon != 45 || lat != 45 {
		t.Fatalf("wantSpans(1) = (%v, %v), want (45, 45)", lon, lat)
	}

	for _, cell := range []string{"0", "e", "s", "z", "sw", "u4", "sw3", "sw33", "sw33j", "u4pru"} {
		for precision := 1; precision <= len(cell); precision++ {
			prefix := cell[:precision]
			b, err := CellBounds(prefix)
			if err != nil {
				t.Fatalf("CellBounds(%q): %v", prefix, err)
			}
			wantLon, wantLat := wantSpans(precision)

			if got := b.MaxLon - b.MinLon; math.Abs(got-wantLon) > 1e-9 {
				t.Errorf("cell %q longitude span = %v, want %v", prefix, got, wantLon)
			}
			if got := b.MaxLat - b.MinLat; math.Abs(got-wantLat) > 1e-9 {
				t.Errorf("cell %q latitude span = %v, want %v", prefix, got, wantLat)
			}
		}
	}
}

// Round trip: the cell decoded from a coordinate must contain that coordinate.
// The subject router depends on this — a stroke's cell has to be reproducible
// from its coordinates, or subscribers on a region never see it.
func TestCellBoundsContainsTheCoordinateThatProducedIt(t *testing.T) {
	const lat, lon = 57.64911, 10.40739

	for precision := 1; precision <= 8; precision++ {
		cell := MustEncode(lat, lon, precision)
		b, err := CellBounds(cell)
		if err != nil {
			t.Fatalf("precision %d: %v", precision, err)
		}
		if !b.Contains(lat, lon) {
			t.Errorf("precision %d: cell %q bounds %+v excludes its own coordinate (%v, %v)",
				precision, cell, b, lat, lon)
		}
	}
}

// A precision-5 cell is this project's NATS subject granularity, so its
// real-world size is a design constraint rather than an academic one. At the
// region's latitude it must be roughly 4 km by 5 km: small enough to keep a
// 10 km circle to a few dozen subjects, large enough that a subject is not
// effectively per-stroke.
func TestCellBoundsPrecision5IsTheSubjectGranularity(t *testing.T) {
	b, err := CellBounds("sw33j")
	if err != nil {
		t.Fatalf("CellBounds: %v", err)
	}
	lonKm := HaversineKm(b.MinLat, b.MinLon, b.MinLat, b.MaxLon)
	latKm := HaversineKm(b.MinLat, b.MinLon, b.MaxLat, b.MinLon)

	if lonKm < 3.5 || lonKm > 4.5 {
		t.Errorf("precision-5 cell is %.2f km wide, want 3.5..4.5 km", lonKm)
	}
	if latKm < 4.5 || latKm > 5.5 {
		t.Errorf("precision-5 cell is %.2f km tall, want 4.5..5.5 km", latKm)
	}
}

func TestCellBoundsRejectsInvalidGeohashes(t *testing.T) {
	for _, cell := range []string{"", "ab", "sw!x", "AB"} {
		t.Run("cell="+cell, func(t *testing.T) {
			if _, err := CellBounds(cell); err == nil {
				t.Errorf("CellBounds(%q) = no error, want error", cell)
			}
		})
	}
}

// Bounds must reject a box that is inverted or out of range, because a silently
// invalid box would filter out everything while reporting itself valid.
func TestBoundsRejectsInvertedAndOutOfRangeBoxes(t *testing.T) {
	for _, tc := range []struct {
		name string
		b    Bounds
	}{
		{"latitudes swapped", Bounds{MinLat: 20, MaxLat: 10, MinLon: 10, MaxLon: 30}},
		{"longitudes swapped", Bounds{MinLat: 10, MaxLat: 30, MinLon: 20, MaxLon: 10}},
		{"latitude past the north pole", Bounds{MinLat: 0, MaxLat: 91, MinLon: 0, MaxLon: 10}},
		{"latitude past the south pole", Bounds{MinLat: -91, MaxLat: 0, MinLon: 0, MaxLon: 10}},
		{"longitude past the dateline east", Bounds{MinLat: 0, MaxLat: 10, MinLon: 0, MaxLon: 181}},
		{"longitude past the dateline west", Bounds{MinLat: 0, MaxLat: 10, MinLon: -181, MaxLon: 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.b.Validate(); err == nil {
				t.Errorf("Validate() on %+v = nil, want error", tc.b)
			}
		})
	}
}

func TestBoundsAcceptsAWellFormedBox(t *testing.T) {
	for _, b := range []Bounds{
		{MinLat: 35.2442, MinLon: 24.3843, MaxLat: 35.4239, MaxLon: 24.6046},
		{MinLat: -90, MinLon: -180, MaxLat: 90, MaxLon: 180},
		{MinLat: 0, MinLon: 0, MaxLat: 0, MaxLon: 0},
	} {
		if err := b.Validate(); err != nil {
			t.Errorf("Validate(%+v) = %v, want nil", b, err)
		}
	}
}

func TestCellCenterReportsTheMidpointOfACell(t *testing.T) {
	// Re-encoding a cell's centre must return that same cell, otherwise the
	// centre is not actually inside its own cell.
	for _, cell := range []string{"sw33j", "u4pru", "s", "0"} {
		lat, lon, err := CellCenter(cell)
		if err != nil {
			t.Fatalf("CellCenter(%q): %v", cell, err)
		}
		if got := MustEncode(lat, lon, len(cell)); got != cell {
			t.Errorf("CellCenter(%q) = (%v, %v), which encodes back to %q", cell, lat, lon, got)
		}
	}
}

func TestCellCenterRejectsInvalidGeohashes(t *testing.T) {
	for _, cell := range []string{"", "not-a-geohash"} {
		if _, _, err := CellCenter(cell); err == nil {
			t.Errorf("CellCenter(%q) = nil error, want error", cell)
		}
	}
}

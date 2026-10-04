package geo

import (
	"math"
	"testing"
)

// Slice 3: HaversineKm measures ground distance between two coordinates.
//
// Distances are checked against values that can be derived independently of
// this implementation: the definition of the metre (1 degree of latitude is
// about 111.19 km), and quarter/half circumference geometry.

func TestHaversineMeasuresZeroForIdenticalPoints(t *testing.T) {
	if got := HaversineKm(35.3340688, 24.4944483, 35.3340688, 24.4944483); got != 0 {
		t.Errorf("identical points = %v km, want 0", got)
	}
}

func TestHaversineMeasuresOneDegreeOfLatitude(t *testing.T) {
	// One degree of latitude is about 111.19 km anywhere on earth.
	got := HaversineKm(0, 0, 1, 0)
	if math.Abs(got-111.19) > 0.5 {
		t.Errorf("one degree of latitude = %.3f km, want ~111.19", got)
	}
}

func TestHaversineMeasuresQuarterCircumference(t *testing.T) {
	// A quarter of the way round the world. The real earth's equatorial
	// circumference is about 40,075 km, so a quarter is about 10,019 km.
	//
	// This implementation models the earth as a sphere of the *mean* radius
	// (6371 km) rather than the equatorial radius (6378 km), which puts the
	// result about 0.1% low. That is the conventional trade-off and is two
	// orders of magnitude finer than the upstream's own ~2 km location
	// uncertainty, so the tolerance here accepts the approximation rather than
	// restating the radius the implementation happens to use.
	const realQuarterEquatorKm = 10018.75

	got := HaversineKm(0, 0, 0, 90)
	relErr := math.Abs(got-realQuarterEquatorKm) / realQuarterEquatorKm
	if relErr > 0.002 {
		t.Errorf("quarter circumference = %.2f km, %.3f%% off the real %.2f km; want within 0.2%%",
			got, relErr*100, realQuarterEquatorKm)
	}
}

func TestHaversineIsSymmetric(t *testing.T) {
	// A distance function that reports different values depending on argument
	// order would silently corrupt every certainty band.
	a := HaversineKm(35.3340688, 24.4944483, 35.5, 24.2)
	b := HaversineKm(35.5, 24.2, 35.3340688, 24.4944483)
	if math.Abs(a-b) > 1e-9 {
		t.Errorf("not symmetric: %v vs %v", a, b)
	}
}

// The distances in this project's own acceptance criteria: the configured
// centre is 10 km from a stroke, so 9.9 km must read as inside and 10.1 km as
// outside.
func TestHaversineDistinguishesInsideFromOutsideTheRegionRadius(t *testing.T) {
	const (
		lat    = 35.3340688
		lon    = 24.4944483
		radius = 10.0
	)

	// One degree of latitude near the region, used to offset by a known amount.
	const kmPerDeg = 111.32

	for _, tc := range []struct {
		name   string
		offset float64 // km north
		wantIn bool
	}{
		{"9.9 km north", 9.9, true},
		{"9.999 km north", 9.999, true},
		{"10.1 km north", 10.1, false},
		{"10.5 km north", 10.5, false},
		{"at the centre", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lat2 := lat + tc.offset/kmPerDeg
			got := HaversineKm(lat, lon, lat2, lon)
			in := got <= radius
			if in != tc.wantIn {
				t.Errorf("distance = %.4f km, inside = %v, want %v", got, in, tc.wantIn)
			}
		})
	}
}

// Longitude degrees shrink toward the poles, so a naive "one degree is one
// degree of latitude" offset in longitude would badly misjudge distance there.
func TestHaversineLongitudeDegreesShrinkWithLatitude(t *testing.T) {
	equator := HaversineKm(0, 0, 0, 1)
	nearPole := HaversineKm(80, 0, 80, 1)

	if nearPole >= equator {
		t.Errorf("a degree of longitude at 80N is %.2f km, but at the equator it is %.2f km; "+
			"expected it to be smaller", nearPole, equator)
	}
	// cos(80 deg) is about 0.174, so the ratio should be near that.
	if ratio := nearPole / equator; math.Abs(ratio-0.1736) > 0.01 {
		t.Errorf("ratio = %.4f, want ~cos(80 deg) = 0.1736", ratio)
	}
}

func TestHaversineHandlesAntipodalPoints(t *testing.T) {
	// Naive float error can push the intermediate value above 1 and make
	// math.Asin return NaN, which would then classify every stroke as
	// "boundary". This asserts the guard holds.
	got := HaversineKm(0, 0, 0, 180)
	if math.IsNaN(got) {
		t.Fatal("HaversineKm returned NaN for antipodal points")
	}
	if math.IsInf(got, 0) {
		t.Fatal("HaversineKm returned Inf for antipodal points")
	}
	if got < 20000 || got > 20020 {
		t.Errorf("antipodal distance = %.1f km, want ~20015", got)
	}
}

func TestHaversineHandlesIdenticalExtremePoints(t *testing.T) {
	// The pole case is where the cosine term vanishes.
	for _, tc := range []struct{ lat, lon float64 }{
		{90, 0}, {-90, 0}, {90, 180}, {-90, -180},
	} {
		if got := HaversineKm(tc.lat, tc.lon, tc.lat, tc.lon); got != 0 {
			t.Errorf("HaversineKm(%v, %v, same) = %v, want 0", tc.lat, tc.lon, got)
		}
	}
}

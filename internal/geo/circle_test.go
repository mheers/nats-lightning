package geo

import (
	"math"
	"testing"
)

// Slice 4: a Circle is the configured region of interest.
//
// The expected bounding-box values are literals computed independently for this
// project's centre and radius: Roussospiti, Crete, at 10 km.

// Roussospiti, Crete — the configured region centre.
const (
	rousLat = 35.3340688
	rousLon = 24.4944483
	rousRad = 10.0
)

func TestCircleRejectsUnusableDefinitions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		circle Circle
	}{
		{"latitude past the pole", Circle{Lat: 90.5, Lon: 0, RadiusKm: 10}},
		{"longitude past the dateline", Circle{Lat: 0, Lon: 180.5, RadiusKm: 10}},
		{"zero radius", Circle{Lat: rousLat, Lon: rousLon, RadiusKm: 0}},
		{"negative radius", Circle{Lat: rousLat, Lon: rousLon, RadiusKm: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.circle.Validate(); err == nil {
				t.Errorf("Validate() on %+v = nil, want error", tc.circle)
			}
		})
	}
}

func TestCircleAcceptsAValidRegion(t *testing.T) {
	for _, c := range []Circle{
		{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad},
		{Lat: 0, Lon: 0, RadiusKm: 0.001},
		{Lat: 90, Lon: 180, RadiusKm: 1},
		{Lat: -90, Lon: -180, RadiusKm: 1},
	} {
		if err := c.Validate(); err != nil {
			t.Errorf("Validate(%+v) = %v, want nil", c, err)
		}
	}
}

// The bounding box is sent upstream as its viewport hint, so its extent decides
// how much of the world the upstream bothers streaming. Getting it wrong in
// either direction is costly: too tight loses strokes, too loose wastes
// bandwidth and invites the upstream's throttling.
func TestCircleBoundingBoxForTheConfiguredRegion(t *testing.T) {
	b := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}.BoundingBox()

	// Each side must reach at least the radius, so no point in the circle can
	// fall outside the requested viewport.
	for _, tc := range []struct {
		name string
		got  float64
		want float64
	}{
		{"north edge", b.MaxLat, 35.4239},
		{"east edge", b.MaxLon, 24.6046},
		{"south edge", b.MinLat, 35.2442},
		{"west edge", b.MinLon, 24.3843},
	} {
		if math.Abs(tc.got-tc.want) > 0.0001 {
			t.Errorf("%s = %.4f, want %.4f", tc.name, tc.got, tc.want)
		}
	}

	if err := b.Validate(); err != nil {
		t.Errorf("BoundingBox() produced an invalid box: %v", err)
	}
}

func TestCircleBoundingBoxContainsTheWholeCircle(t *testing.T) {
	for _, c := range []Circle{
		{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad},
		{Lat: 0, Lon: 0, RadiusKm: 500},
		{Lat: 68, Lon: 25, RadiusKm: 50}, // high latitude
		{Lat: -33, Lon: 151, RadiusKm: 25},
	} {
		b := c.BoundingBox()
		// Walk the circle's own edge in 360 steps; every one must be requested.
		for deg := range 360 {
			angle := float64(deg) * math.Pi / 180
			lat := c.Lat + (c.RadiusKm*math.Sin(angle))/kmPerDegLat
			lon := c.Lon + (c.RadiusKm*math.Cos(angle))/(kmPerDegLat*math.Cos(c.Lat*math.Pi/180))
			if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
				continue // wrapped off the globe
			}
			if !b.Contains(lat, lon) {
				t.Fatalf("circle %+v: point (%.5f, %.5f) at %d deg is outside its own bounding box %+v",
					c, lat, lon, deg, b)
			}
		}
	}
}

// Longitude degrees are shorter than latitude degrees away from the equator, so
// a box built from the same degree count in both axes would be far too narrow
// in longitude at high latitude and silently drop strokes.
func TestCircleBoundingBoxWidensInLongitudeAwayFromTheEquator(t *testing.T) {
	equator := Circle{Lat: 0, Lon: 0, RadiusKm: rousRad}.BoundingBox()
	high := Circle{Lat: 60, Lon: 0, RadiusKm: rousRad}.BoundingBox()

	equatorLonDeg := equator.MaxLon - equator.MinLon
	highLonDeg := high.MaxLon - high.MinLon

	if highLonDeg <= equatorLonDeg {
		t.Errorf("at 60N the longitude span is %.4f deg but at the equator it is %.4f deg; "+
			"expected it to be wider in degrees to cover the same distance",
			highLonDeg, equatorLonDeg)
	}
	// cos(60) = 0.5, so the span should be about double.
	if ratio := highLonDeg / equatorLonDeg; math.Abs(ratio-2) > 0.01 {
		t.Errorf("longitude span ratio at 60N = %.3f, want ~2 (= 1/cos 60)", ratio)
	}
}

// A circle at the pole would divide by a vanishing cosine. The box must stay
// finite and usable rather than overflowing to infinity.
func TestCircleBoundingBoxClampsAtThePoles(t *testing.T) {
	for _, lat := range []float64{90, -90, 89.9999} {
		c := Circle{Lat: lat, Lon: 0, RadiusKm: rousRad}
		b := c.BoundingBox()

		if math.IsInf(b.MaxLon, 0) || math.IsInf(b.MinLon, 0) {
			t.Errorf("latitude %v produced an infinite bounding box: %+v", lat, b)
		}
		if b.MaxLat > 90 || b.MinLat < -90 {
			t.Errorf("latitude %v produced an out-of-range bounding box: %+v", lat, b)
		}
	}
}

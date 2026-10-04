package geo

import (
	"fmt"
	"math"
	"testing"
)

// describeCircle names a subtest after the circle, so a failure says which region
// broke rather than repeating an identical subtest name.
func describeCircle(c Circle) string {
	return fmt.Sprintf("%.4f,%.4f r%gkm", c.Lat, c.Lon, c.RadiusKm)
}

// sampleCircle returns points spread across the whole disc: a spiral in polar
// coordinates rather than a ring, so the interior is covered as well as the edge.
//
// Points are checked against ContainsPoint by the caller, so the sampling does not
// have to be exact about staying inside — it only has to put points near it.
func sampleCircle(c Circle, n int) [][2]float64 {
	out := make([][2]float64, 0, n)
	const twoPi = 2 * math.Pi

	for i := 0; i < n; i++ {
		theta := twoPi * float64(i) / float64(n)
		// Golden-angle stepping keeps successive radii from lining up, so the
		// samples do not trace a spiral that repeatedly revisits the same cells.
		radiusFrac := math.Mod(float64(i)*0.618033988749895, 1)

		distKm := c.RadiusKm * 1.02 * math.Sqrt(radiusFrac)
		dLat := distKm * math.Sin(theta) / kmPerDegLat
		lat := c.Lat + dLat
		if lat > 90 {
			lat = 90
		}
		if lat < -90 {
			lat = -90
		}

		remaining := distKm * math.Cos(theta)
		lon := c.Lon + remaining/cosLatKm(lat)
		// Normalise the way a real longitude arrives, so a region that wraps
		// produces the negative longitudes it really contains.
		lon = math.Mod(lon+180, 360)
		if lon < 0 {
			lon += 360
		}
		lon -= 180

		out = append(out, [2]float64{lat, lon})
	}
	return out
}

// TestSampleCircleIsRepresentative keeps the helper honest: if the sampler stopped
// producing points near an ordinary region, the coverage assertions above would
// pass vacuously by enumerating very little.
func TestSampleCircleIsRepresentative(t *testing.T) {
	c := Circle{Lat: 35.3340688, Lon: 24.4944483, RadiusKm: 10}

	inside, near := 0, 0
	for _, p := range sampleCircle(c, 500) {
		if c.ContainsPoint(p[0], p[1]) {
			inside++
		} else if HaversineKm(c.Lat, c.Lon, p[0], p[1]) <= c.RadiusKm*1.15 {
			near++
		}
	}
	if inside < 300 {
		t.Errorf("only %d of 500 samples landed inside the circle; the coverage tests "+
			"would pass without testing much", inside)
	}
	if inside+near < 450 {
		t.Errorf("only %d of 500 samples landed near the circle at all", inside+near)
	}
}

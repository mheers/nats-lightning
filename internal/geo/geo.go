// Package geo provides the spatial primitives the feed needs: geohash encoding
// for NATS subject routing, and circle containment for the configured region.
package geo

import (
	"fmt"
	"math"
)

// alphabet is the standard geohash base-32 alphabet. It excludes a, i, l and o
// to avoid transcription errors.
const alphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

// encodeBits are the five bits each geohash character contributes, most
// significant first. They alternate longitude, latitude, longitude, latitude,
// longitude.
var encodeBits = [5]byte{16, 8, 4, 2, 1}

// Encode returns the geohash of lat/lon at the requested precision (1..12).
//
// Each character contributes five bits, interleaved starting with longitude.
// Because the interleave is three longitude bits and two latitude bits, one
// extra character of precision divides the longitude span by 8 and the
// latitude span by 4, so the cell area shrinks by a factor of 32.
func Encode(lat, lon float64, precision int) (string, error) {
	if precision < 1 || precision > 12 {
		return "", fmt.Errorf("geo: precision %d out of range 1..12", precision)
	}
	if lat < -90 || lat > 90 {
		return "", fmt.Errorf("geo: latitude %v out of range [-90, 90]", lat)
	}
	if lon < -180 || lon > 180 {
		return "", fmt.Errorf("geo: longitude %v out of range [-180, 180]", lon)
	}

	latLo, latHi := -90.0, 90.0
	lonLo, lonHi := -180.0, 180.0
	out := make([]byte, 0, precision)
	even := true // even steps bisect longitude, odd steps bisect latitude

	for range precision {
		var idx int
		for _, mask := range encodeBits {
			if even { // longitude
				mid := (lonLo + lonHi) / 2
				if lon > mid {
					idx |= int(mask)
					lonLo = mid
				} else {
					lonHi = mid
				}
			} else { // latitude
				mid := (latLo + latHi) / 2
				if lat > mid {
					idx |= int(mask)
					latLo = mid
				} else {
					latHi = mid
				}
			}
			even = !even
		}
		out = append(out, alphabet[idx])
	}
	return string(out), nil
}

// MustEncode is Encode for call sites where the inputs are known good, such as
// tests and fixtures. It panics on error.
func MustEncode(lat, lon float64, precision int) string {
	g, err := Encode(lat, lon, precision)
	if err != nil {
		panic(err)
	}
	return g
}

// Bounds is a geographic bounding box in degrees.
type Bounds struct {
	MinLat float64
	MinLon float64
	MaxLat float64
	MaxLon float64
}

// Contains reports whether the point lies within the bounds, inclusive.
func (b Bounds) Contains(lat, lon float64) bool {
	return lat >= b.MinLat && lat <= b.MaxLat && lon >= b.MinLon && lon <= b.MaxLon
}

// CellBounds returns the geographic extent of the geohash cell.
func CellBounds(cell string) (Bounds, error) {
	if cell == "" {
		return Bounds{}, fmt.Errorf("geo: empty geohash")
	}
	latLo, latHi := -90.0, 90.0
	lonLo, lonHi := -180.0, 180.0
	even := true

	for i := range len(cell) {
		idx := -1
		for j := range len(alphabet) {
			if alphabet[j] == cell[i] {
				idx = j
				break
			}
		}
		if idx < 0 {
			return Bounds{}, fmt.Errorf("geo: invalid geohash character %q in %q", cell[i], cell)
		}
		for _, mask := range encodeBits {
			bit := idx & int(mask)
			if even { // longitude
				mid := (lonLo + lonHi) / 2
				if bit != 0 {
					lonLo = mid
				} else {
					lonHi = mid
				}
			} else { // latitude
				mid := (latLo + latHi) / 2
				if bit != 0 {
					latLo = mid
				} else {
					latHi = mid
				}
			}
			even = !even
		}
	}
	return Bounds{MinLat: latLo, MinLon: lonLo, MaxLat: latHi, MaxLon: lonHi}, nil
}

// earthRadiusKm is the mean earth radius, the value a spherical approximation
// to the haversine formula conventionally uses.
const earthRadiusKm = 6371.0088

// HaversineKm returns the great-circle distance in kilometres between two
// points on a sphere.
//
// The spherical approximation is accurate to roughly 0.3% against an ellipsoid,
// which is far below the upstream's own location uncertainty of around 2 km, so
// a more exact formula would buy nothing here.
func HaversineKm(lat1, lon1, lat2, lon2 float64) float64 {
	const toRad = math.Pi / 180

	φ1 := lat1 * toRad
	φ2 := lat2 * toRad
	dφ := (lat2 - lat1) * toRad
	dλ := (lon2 - lon1) * toRad

	a := math.Sin(dφ/2)*math.Sin(dφ/2) +
		math.Cos(φ1)*math.Cos(φ2)*math.Sin(dλ/2)*math.Sin(dλ/2)

	// The clamp guards against floating point error pushing a above 1, which
	// would make Asin return NaN for coincident or antipodal points.
	return 2 * earthRadiusKm * math.Asin(math.Sqrt(math.Min(1, a)))
}

// Validate reports whether the bounds are well-formed and non-empty.
func (b Bounds) Validate() error {
	if b.MinLat < -90 || b.MaxLat > 90 {
		return fmt.Errorf("geo: latitude out of range: %v..%v", b.MinLat, b.MaxLat)
	}
	if b.MinLon < -180 || b.MaxLon > 180 {
		return fmt.Errorf("geo: longitude out of range: %v..%v", b.MinLon, b.MaxLon)
	}
	if b.MinLat > b.MaxLat {
		return fmt.Errorf("geo: min lat %v exceeds max lat %v", b.MinLat, b.MaxLat)
	}
	if b.MinLon > b.MaxLon {
		return fmt.Errorf("geo: min lon %v exceeds max lon %v", b.MinLon, b.MaxLon)
	}
	return nil
}

// kmPerDegLat is the length of one degree of latitude. It varies by about 0.3%
// across the globe, which is immaterial at the scales this package works with.
const kmPerDegLat = 111.32

// Circle is a region of interest: every point within RadiusKm of the centre.
type Circle struct {
	Lat      float64
	Lon      float64
	RadiusKm float64
}

// Validate reports whether the circle is usable.
func (c Circle) Validate() error {
	if c.Lat < -90 || c.Lat > 90 {
		return fmt.Errorf("geo: circle latitude %v out of range [-90, 90]", c.Lat)
	}
	if c.Lon < -180 || c.Lon > 180 {
		return fmt.Errorf("geo: circle longitude %v out of range [-180, 180]", c.Lon)
	}
	if c.RadiusKm <= 0 {
		return fmt.Errorf("geo: circle radius %v must be positive", c.RadiusKm)
	}
	return nil
}

// BoundingBox returns the smallest axis-aligned box containing the circle.
//
// This is what gets sent upstream as the viewport hint, so it must contain the
// whole circle; a box that clipped it would silently drop strokes.
//
// The longitude half-width widens toward the poles because meridians converge
// there. The cosine is floored so the result stays finite at the poles instead
// of dividing by zero.
func (c Circle) BoundingBox() Bounds {
	dLat := c.RadiusKm / kmPerDegLat
	dLon := c.RadiusKm / cosLatKm(c.Lat)

	return Bounds{
		MinLat: math.Max(-90, c.Lat-dLat),
		MinLon: math.Max(-180, c.Lon-dLon),
		MaxLat: math.Min(90, c.Lat+dLat),
		MaxLon: math.Min(180, c.Lon+dLon),
	}
}

// ContainsPoint reports whether the point lies inside the circle, ignoring the
// upstream's reported location uncertainty. Certainty-aware filtering lives in
// certainty.go.
func (c Circle) ContainsPoint(lat, lon float64) bool {
	return HaversineKm(c.Lat, c.Lon, lat, lon) <= c.RadiusKm
}

// cosLatKm is kilometres per degree of longitude at the given latitude.
// The cosine is floored so the result stays finite at the poles.
func cosLatKm(lat float64) float64 {
	cosLat := math.Abs(math.Cos(lat * math.Pi / 180))
	if cosLat < 1e-9 {
		return 1e-9
	}
	return kmPerDegLat * cosLat
}

// Cells returns every geohash at the given precision whose bounding box
// intersects the circle's bounding box.
//
// Selection is by bounding-box intersection rather than true circle-to-cell
// overlap, which makes the result a deliberate superset: a cell that turns out
// unnecessary costs a little wasted bandwidth, whereas a missing cell silently
// drops lightning strokes. Erring generous is the only safe direction.
//
// The geohash tree is walked from the single root cell, pruning any subtree
// whose box misses the target, so cost scales with the number of cells returned
// rather than with the size of the world.
func (c Circle) Cells(precision int) ([]string, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if precision < 1 || precision > 12 {
		return nil, fmt.Errorf("geo: precision %d out of range 1..12", precision)
	}

	target := c.BoundingBox()
	if err := target.Validate(); err != nil {
		return nil, err
	}

	var out []string

	// The walk starts at the geohash root, which is the whole world. Handing
	// the empty string to CellBounds would be rejected as invalid, so the root
	// is handled here rather than by loosening that validation.
	const rootCell = ""
	rootBounds := Bounds{MinLat: -90, MinLon: -180, MaxLat: 90, MaxLon: 180}

	var walk func(cell string, b Bounds) error
	walk = func(cell string, b Bounds) error {
		// No overlap with the target box: prune this entire subtree.
		if b.MaxLon < target.MinLon || b.MinLon > target.MaxLon ||
			b.MaxLat < target.MinLat || b.MinLat > target.MaxLat {
			return nil
		}
		if len(cell) == precision {
			out = append(out, cell)
			return nil
		}
		for i := range len(alphabet) {
			child := cell + alphabet[i:i+1]
			childBounds, err := CellBounds(child)
			if err != nil {
				return err
			}
			if err := walk(child, childBounds); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(rootCell, rootBounds); err != nil {
		return nil, err
	}
	return out, nil
}

// InCells reports whether a geohash is in the set. An empty set means no
// restriction, which is how a world-wide deployment runs.
func InCells(cells map[string]struct{}, cell string) bool {
	if len(cells) == 0 {
		return true
	}
	_, ok := cells[cell]
	return ok
}

// ToSet converts a cell list to a set for constant-time lookups. It returns nil
// for an empty list so callers can distinguish "no restriction" from "some
// cells, none of which match".
func ToSet(cells []string) map[string]struct{} {
	if len(cells) == 0 {
		return nil
	}
	m := make(map[string]struct{}, len(cells))
	for _, c := range cells {
		m[c] = struct{}{}
	}
	return m
}

// CellCenter returns the midpoint of a geohash cell.
func CellCenter(cell string) (lat, lon float64, err error) {
	b, err := CellBounds(cell)
	if err != nil {
		return 0, 0, err
	}
	return (b.MinLat + b.MaxLat) / 2, (b.MinLon + b.MaxLon) / 2, nil
}

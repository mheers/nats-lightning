// Package geo provides the spatial primitives the feed needs: geohash encoding
// for NATS subject routing, and circle containment for the configured region.
package geo

import (
	"errors"
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
// MaxRadiusKm bounds a region so it stays a region.
//
// This is a cheap sanity bound, and it is deliberately not the memory bound: a
// degree of longitude covers fewer kilometres the further you get from the equator,
// so the same radius needs progressively more cells. Measured at precision 5, for a
// 2500 km radius, the latitude column matters more than anything an operator types:
//
//	latitude   cells      heap
//	     0°    1.1 M     218 MiB
//	    30°    1.7 M     249 MiB
//	    45°    2.7 M     486 MiB
//	    60°    8.0 M   1523 MiB
//	    70°    7.9 M   1522 MiB
//
// So the cost is bounded where it is actually incurred — see MaxCells — rather than
// here.
const MaxRadiusKm = 2500

// MaxCells bounds what enumerating a region's cells may cost.
//
// It is enforced inside Cells rather than in Validate because the cell count is the
// quantity that matters and it is not predictable from the radius alone. Bounding a
// proxy instead is what this constant replaced, and the proxy was wrong in both
// directions: it rejected a 1 km circle at the pole, which is one of the cheapest
// regions there is — all longitudes meet at the pole, so its longitude span is the
// whole globe while its cells number in the thousands — and it was the only thing
// standing between a Scandinavian 2500 km region and a 1.5 GiB allocation.
//
// The result is paid three times over: the cell list is built in memory, written to
// the region table as JSON, and re-parsed by json_each on every radius query. Two
// million cells is about a quarter of a gigabyte, which is the most this process has
// ever needed.
const MaxCells = 2_000_000

// Validate checks a circle is usable as a region.
//
// Only geometry is checked here. Whether the enumeration is affordable is decided by
// Cells, which is the only place that can know.
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
	if c.RadiusKm > MaxRadiusKm {
		return fmt.Errorf(
			"geo: circle radius %v km exceeds the %v km maximum. Omit the region "+
				"entirely to publish world-wide",
			c.RadiusKm, MaxRadiusKm)
	}
	return nil
}

// BoundingBoxes returns boxes whose union covers the circle.
//
// It is almost always one box, but a circle centred near longitude ±180 is two:
// its longitude span runs off one edge of the coordinate space and reappears at the
// other. Clamping the span instead — the obvious thing — silently drops the half of
// the region on the far side, and because cell enumeration is a superset by design
// everywhere else, that is the one place where erring generous quietly inverts into
// erring lossy. A 200 km circle at longitude 179.5 enumerated 4565 cells and lost
// 28% of its own area; every stroke there was dropped as "outside the region" and
// invisible to the radius query, while the metric blamed the region for it.
//
// The longitude half-width widens toward the poles because meridians converge
// there, and the cosine is floored so the result stays finite at the poles.
func (c Circle) BoundingBoxes() []Bounds {
	dLat := c.RadiusKm / kmPerDegLat
	minLat := math.Max(-90, c.Lat-dLat)
	maxLat := math.Min(90, c.Lat+dLat)

	// The longitude half-width is set by the narrowest degree of longitude in the
	// span, which is the one furthest from the equator — not the one at the centre.
	// Measuring at the centre under-covers the poleward side of the circle, because a
	// degree of longitude there covers fewer kilometres and so more degrees are needed
	// to span the same distance. A 500 km circle at 70°N lost 6 cells' worth of its
	// own area this way, which is the same failure as the seam below: a stroke inside
	// the region, filed in a cell the region does not list, dropped as
	// outside_region.
	widest := math.Max(math.Abs(minLat), math.Abs(maxLat))
	dLon := c.RadiusKm / cosLatKm(widest)

	// Near a pole the cosine floor makes dLon enormous: a 10 km circle at latitude
	// 90 spans about 9e7 degrees of longitude, which is many times the whole globe.
	// Wrapping that is meaningless — there is nothing left outside it — and
	// arithmetic on it produced boxes reaching past ±180, so Cells rejected the
	// output of a circle that Validate had just accepted. Once the span covers the
	// world the answer is simply the world.
	if dLon >= 180 {
		return []Bounds{{MinLat: minLat, MaxLat: maxLat, MinLon: -180, MaxLon: 180}}
	}

	// With dLon < 180 the span is under 360 degrees, so it can cross at most one
	// edge and both resulting boxes land inside [-180, 180].
	minLon, maxLon := c.Lon-dLon, c.Lon+dLon

	switch {
	case minLon < -180:
		// Runs off the western edge and wraps to the east.
		return []Bounds{
			{MinLat: minLat, MaxLat: maxLat, MinLon: minLon + 360, MaxLon: 180},
			{MinLat: minLat, MaxLat: maxLat, MinLon: -180, MaxLon: maxLon},
		}
	case maxLon > 180:
		// Runs off the eastern edge and wraps to the west.
		return []Bounds{
			{MinLat: minLat, MaxLat: maxLat, MinLon: minLon, MaxLon: 180},
			{MinLat: minLat, MaxLat: maxLat, MinLon: -180, MaxLon: maxLon - 360},
		}
	default:
		return []Bounds{{MinLat: minLat, MaxLat: maxLat, MinLon: minLon, MaxLon: maxLon}}
	}
}

// BoundingBox returns the smallest axis-aligned box containing the circle.
//
// This is what gets sent upstream as the viewport hint, so it must contain the
// whole circle; a box that clipped it would silently drop strokes. It is the
// un-wrapped form, which is what a viewport wants: a subscriber selecting across
// the seam is one contiguous request, not two.
//
// Use BoundingBoxes when enumerating cells, where the wrap has to be honoured.
func (c Circle) BoundingBox() Bounds {
	dLat := c.RadiusKm / kmPerDegLat
	minLat := math.Max(-90, c.Lat-dLat)
	maxLat := math.Min(90, c.Lat+dLat)
	widest := math.Max(math.Abs(minLat), math.Abs(maxLat))
	dLon := c.RadiusKm / cosLatKm(widest)

	return Bounds{
		MinLat: minLat,
		MinLon: math.Max(-180, c.Lon-dLon),
		MaxLat: maxLat,
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
//
// A circle crossing the antimeridian has two target boxes rather than one, and the
// walks are unioned: the boxes are disjoint, but a cell is only ever appended once
// so the result is still a set. The order is unspecified because callers treat it as
// a set — publisher.AllowsCell and the store's IN clause both do.
func (c Circle) Cells(precision int) ([]string, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if precision < 1 || precision > 12 {
		return nil, fmt.Errorf("geo: precision %d out of range 1..12", precision)
	}

	targets := c.BoundingBoxes()
	for _, t := range targets {
		if err := t.Validate(); err != nil {
			return nil, err
		}
	}

	var out []string
	seen := make(map[string]struct{})

	// The walk starts at the geohash root, which is the whole world. Handing
	// the empty string to CellBounds would be rejected as invalid, so the root
	// is handled here rather than by loosening that validation.
	const rootCell = ""
	rootBounds := Bounds{MinLat: -90, MinLon: -180, MaxLat: 90, MaxLon: 180}

	for _, target := range targets {
		var walk func(cell string, b Bounds) error
		walk = func(cell string, b Bounds) error {
			// No overlap with the target box: prune this entire subtree.
			if b.MaxLon < target.MinLon || b.MinLon > target.MaxLon ||
				b.MaxLat < target.MinLat || b.MinLat > target.MaxLat {
				return nil
			}
			if len(cell) == precision {
				if _, dup := seen[cell]; !dup {
					// Budget checked before the append, so the walk never allocates
					// more than MaxCells however large the region is. The tree is
					// pruned above, so an over-budget region costs the walk and then
					// stops, rather than the allocation it was heading for.
					if len(out) >= MaxCells {
						return errTooManyCells
					}
					seen[cell] = struct{}{}
					out = append(out, cell)
				}
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
			if errors.Is(err, errTooManyCells) {
				return nil, fmt.Errorf(
					"geo: a region of %.4f,%.4f with radius %v km needs more than %d "+
						"geohash-%d cells, which is more than this bridge will allocate. "+
						"Use a smaller radius, or omit the region to publish world-wide",
					c.Lat, c.Lon, c.RadiusKm, MaxCells, precision)
			}
			return nil, err
		}
	}
	return out, nil
}

// errTooManyCells aborts the cell walk when a region exceeds MaxCells. It is
// distinguished from other walk errors only so the caller can explain the cost.
var errTooManyCells = errors.New("geo: region exceeds the cell budget")

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

package geo

import "fmt"

// Certainty classifies how confidently a stroke lies inside a region.
//
// The upstream reports each stroke with a `dev` deviation in metres: the
// network's own estimate of how far the reported position may sit from the true
// strike. Measured across the network, that deviation has a median around
// 2.2 km and reaches 15 km. Once it approaches the region's radius, "is this
// stroke inside?" stops being decidable from the data: a stroke reported 9.9 km
// out with 2 km of uncertainty may physically be well inside, and one reported
// 0.5 km out with 15 km of uncertainty may physically be far outside.
//
// Rather than collapsing that ambiguity into a boolean, every stroke is
// classified so the decision is visible and reversible.
type Certainty string

const (
	// CertaintyIn means the stroke is inside the circle even after allowing
	// for the full reported deviation: distance + deviation <= radius.
	CertaintyIn Certainty = "in"

	// CertaintyBoundary means the stroke is genuinely undecidable:
	// distance - deviation <= radius < distance + deviation.
	CertaintyBoundary Certainty = "boundary"

	// CertaintyOut means the stroke is outside the circle even after allowing
	// for the full reported deviation: distance - deviation > radius.
	CertaintyOut Certainty = "out"
)

// BoundaryPolicy decides whether undecidable strokes are treated as inside.
type BoundaryPolicy string

const (
	// PolicyInclude counts boundary strokes as inside the region.
	//
	// This is the default for this project. The region answers "was there
	// lightning near the village", where a missed stroke is worse than a
	// slightly misplaced one.
	PolicyInclude BoundaryPolicy = "include"

	// PolicyExclude counts only certainly-inside strokes.
	PolicyExclude BoundaryPolicy = "exclude"
)

// ParseBoundaryPolicy validates a policy from configuration.
func ParseBoundaryPolicy(s string) (BoundaryPolicy, error) {
	switch p := BoundaryPolicy(s); p {
	case PolicyInclude, PolicyExclude:
		return p, nil
	default:
		return "", fmt.Errorf("geo: boundary policy must be %q or %q, got %q",
			PolicyInclude, PolicyExclude, s)
	}
}

// MaxPlausibleDeviationM bounds how large a reported location uncertainty may be.
//
// The asymmetry is the point: a negative figure is clamped to zero here, and an
// enormous one was not bounded at all. DeviationM is an *int, so INT_MAX — the
// most common "no data" sentinel in this family of protocols — is representable,
// and every stroke within radius + dev then classifies as CertaintyBoundary. The
// default PolicyInclude accepts boundary, so one stroke carrying a sentinel makes
// every stroke on earth publish: the region filter stops filtering, no counter
// moves, and /healthz still reports the region as configured.
//
// The observed range is 300–15000 m, so 100 km is two orders of magnitude of
// headroom over anything the upstream has actually sent. The bound belongs here
// rather than only at the decoder because this function is the last thing every
// caller passes through, and a caller reading the archive or a future transport
// must not be able to bypass it.
const MaxPlausibleDeviationM = 100_000

// Classify determines a stroke's band from its distance from the centre and the
// upstream's reported deviation in metres.
//
// A negative deviation is treated as zero and one above
// MaxPlausibleDeviationM is capped at it: an unusable uncertainty figure must not
// manufacture ambiguity that the data does not support, in either direction.
func (c Circle) Classify(distKm float64, deviationM float64) Certainty {
	devKm := deviationM / 1000
	switch {
	case devKm < 0:
		devKm = 0
	case devKm > MaxPlausibleDeviationM/1000:
		devKm = MaxPlausibleDeviationM / 1000
	}
	switch {
	case distKm+devKm <= c.RadiusKm:
		return CertaintyIn
	case distKm-devKm <= c.RadiusKm:
		return CertaintyBoundary
	default:
		return CertaintyOut
	}
}

// ClassifyPoint is Classify for callers holding coordinates rather than a
// precomputed distance.
func (c Circle) ClassifyPoint(lat, lon, deviationM float64) (distKm float64, certainty Certainty) {
	distKm = HaversineKm(c.Lat, c.Lon, lat, lon)
	return distKm, c.Classify(distKm, deviationM)
}

// Contains reports whether a stroke counts as inside the region under the given
// policy.
//
// A zero-value circle means no region is configured, in which case nothing is
// filtered: a deployment that forgot its coordinates must pass data through
// rather than silently drop every stroke.
func (c Circle) Contains(lat, lon, deviationM float64, policy BoundaryPolicy) bool {
	if c.RadiusKm <= 0 {
		return true
	}
	certainty := c.Classify(HaversineKm(c.Lat, c.Lon, lat, lon), deviationM)
	switch certainty {
	case CertaintyIn:
		return true
	case CertaintyBoundary:
		return policy == PolicyInclude
	default:
		return false
	}
}

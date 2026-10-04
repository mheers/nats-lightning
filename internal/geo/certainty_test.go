package geo

import "testing"

// Slice 6: certainty bands.
//
// The upstream reports each stroke with a `dev` deviation in metres: its own
// estimate of how far the reported position may sit from the true strike. When
// that approaches the region's radius, "is this stroke inside?" stops being a
// decidable question, and collapsing that into a boolean would quietly invent
// certainty the data does not have.
//
// The tests below are written in terms of what a caller observes: which band a
// stroke lands in, and whether it counts as inside under each policy.

func TestStrokesWellInsideTheRegionAreCertainlyInside(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}

	for _, tc := range []struct {
		name       string
		distKm     float64
		deviationM float64
	}{
		{"at the centre with no deviation", 0, 0},
		{"at the centre with 8 km of deviation", 0, 8000},
		{"5 km out with 1 km of deviation", 5, 1000},
		{"8 km out with 1 km of deviation", 8, 1000},
		{"exactly on the radius with no deviation", 10, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.Classify(tc.distKm, tc.deviationM)
			if got != CertaintyIn {
				t.Errorf("Classify(dist=%.1f, dev=%.0fm) = %q, want %q",
					tc.distKm, tc.deviationM, got, CertaintyIn)
			}
			// A certainly-inside stroke counts as inside under either policy.
			if !c.Contains(northOf(c.Lat, tc.distKm), c.Lon, tc.deviationM, PolicyExclude) {
				t.Error("a certainly-inside stroke must count as inside under PolicyExclude")
			}
		})
	}
}

// A stroke whose deviation straddles the radius is genuinely undecidable: it may
// physically be inside or physically be outside. This is the case the whole
// band mechanism exists for.
func TestStrokesStraddlingTheRadiusAreBoundary(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}

	for _, tc := range []struct {
		name       string
		distKm     float64
		deviationM float64
	}{
		{"9 km out with 2 km of deviation", 9, 2000},
		{"on the radius with 2 km of deviation", 10, 2000},
		{"12 km out with 4 km of deviation", 12, 4000},
		{"6 km out with 5 km of deviation", 6, 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.Classify(tc.distKm, tc.deviationM)
			if got != CertaintyBoundary {
				t.Errorf("Classify(dist=%.1f, dev=%.0fm) = %q, want %q",
					tc.distKm, tc.deviationM, got, CertaintyBoundary)
			}
		})
	}
}

func TestStrokesBeyondTheDeviationAreCertainlyOutside(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}

	for _, tc := range []struct {
		name       string
		distKm     float64
		deviationM float64
	}{
		{"11 km out with no deviation", 11, 0},
		{"15 km out with 1 km of deviation", 15, 1000},
		{"20 km out with 5 km of deviation", 20, 5000},
		{"50 km out", 50, 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.Classify(tc.distKm, tc.deviationM)
			if got != CertaintyOut {
				t.Errorf("Classify(dist=%.1f, dev=%.0fm) = %q, want %q",
					tc.distKm, tc.deviationM, got, CertaintyOut)
			}
			if c.Contains(northOf(c.Lat, tc.distKm), c.Lon, tc.deviationM, PolicyInclude) {
				t.Error("a certainly-outside stroke must not count as inside, even under PolicyInclude")
			}
		})
	}
}

// The decided policy: a boundary stroke counts as inside, because for this
// project the circle answers "was there lightning near the village" and a
// missed stroke is worse than a slightly misplaced one.
func TestPolicyIncludeCountsBoundaryStrokesAsInside(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}
	lat, lon := northOf(c.Lat, 9), c.Lon
	const deviationM = 2000

	if !c.Contains(lat, lon, deviationM, PolicyInclude) {
		t.Error("PolicyInclude must accept a boundary stroke")
	}
	if c.Contains(lat, lon, deviationM, PolicyExclude) {
		t.Error("PolicyExclude must reject a boundary stroke")
	}
}

// The decision must stay reversible at runtime, so the band has to be
// observable rather than folded into the filter.
func TestBoundaryStrokesRemainIdentifiableRegardlessOfPolicy(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}
	lat, lon := northOf(c.Lat, 9), c.Lon
	const deviationM = 2000

	for _, policy := range []BoundaryPolicy{PolicyInclude, PolicyExclude} {
		_, certainty := c.ClassifyPoint(lat, lon, deviationM)
		if certainty != CertaintyBoundary {
			t.Errorf("under policy %q the band was %q, want %q: it must stay observable",
				policy, certainty, CertaintyBoundary)
		}
	}
}

// An absent or nonsensical deviation must not manufacture false ambiguity. A
// stroke 5 km out reported with dev = -1 is better described as certainly in
// than as undecidable.
func TestUnusableDeviationDoesNotCreateFalseAmbiguity(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}
	lat, lon := northOf(c.Lat, 5), c.Lon

	for _, dev := range []float64{0, -1, -5000} {
		if got := c.Classify(5, dev); got != CertaintyIn {
			t.Errorf("Classify(5, dev=%v) = %q, want %q", dev, got, CertaintyIn)
		}
		if !c.Contains(lat, lon, dev, PolicyExclude) {
			t.Errorf("dev=%v must not exclude a stroke 5 km from a 10 km region", dev)
		}
	}
}

// An unconfigured region must not filter anything out. Otherwise a deployment
// that forgot to set coordinates would silently drop every stroke.
func TestUnconfiguredRegionPassesEveryStrokeThrough(t *testing.T) {
	var c Circle // zero value: no region configured

	for _, policy := range []BoundaryPolicy{PolicyInclude, PolicyExclude} {
		for _, tc := range []struct{ lat, lon float64 }{
			{rousLat, rousLon},
			{0, 0},
			{-89, 179},
			{51.5, -0.12},
		} {
			if !c.Contains(tc.lat, tc.lon, 0, policy) {
				t.Errorf("unconfigured circle rejected (%v, %v) under policy %q", tc.lat, tc.lon, policy)
			}
		}
	}
}

// ClassifyPoint must agree with Classify: the two-argument form exists for
// callers that already hold a distance.
func TestClassifyPointAgreesWithClassify(t *testing.T) {
	c := Circle{Lat: rousLat, Lon: rousLon, RadiusKm: rousRad}

	for _, tc := range []struct{ distKm, deviationM float64 }{
		{0, 0}, {3, 500}, {9, 2000}, {9, 0}, {12, 1000}, {25, 0},
	} {
		lat, lon := northOf(c.Lat, tc.distKm), c.Lon
		dist, certainty := c.ClassifyPoint(lat, lon, tc.deviationM)

		if want := HaversineKm(c.Lat, c.Lon, lat, lon); dist < want-0.01 || dist > want+0.01 {
			t.Errorf("ClassifyPoint distance = %v, want ~%v", dist, want)
		}
		if got := c.Classify(tc.distKm, tc.deviationM); certainty != got {
			t.Errorf("ClassifyPoint band %q disagrees with Classify %q for dist=%.1f dev=%v",
				certainty, got, tc.distKm, tc.deviationM)
		}
	}
}

func TestParseBoundaryPolicyRejectsUnknownValues(t *testing.T) {
	for _, in := range []string{"", "INCLUDE", "maybe", "include "} {
		if _, err := ParseBoundaryPolicy(in); err == nil {
			t.Errorf("ParseBoundaryPolicy(%q) = nil error, want error", in)
		}
	}
	for _, in := range []string{"include", "exclude"} {
		if _, err := ParseBoundaryPolicy(in); err != nil {
			t.Errorf("ParseBoundaryPolicy(%q) = %v, want nil", in, err)
		}
	}
}

// northOf returns a latitude tcKm north of lat, using the same conversion the
// production code uses so the test reads in kilometres rather than degrees.
func northOf(lat, km float64) float64 {
	return lat + km/kmPerDegLat
}

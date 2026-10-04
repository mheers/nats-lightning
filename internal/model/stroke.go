// Package model holds the normalised representation of a lightning stroke.
//
// The upstream wire format is undocumented and changes without notice, and its
// two transports disagree about types: the WebSocket sends coordinates as JSON
// numbers while the HTTP long-poll endpoint sends them as strings. Normalising
// here means no consumer ever has to know either of those facts.
package model

import (
	"fmt"
	"time"
)

// Source identifies which network reported a stroke.
type Source int

const (
	// SourceBlitzortung is the Blitzortung.org computing network.
	SourceBlitzortung Source = 1

	// SourceLightningMaps is the LightningMaps.org network. This is the
	// upstream's own default and the one this project subscribes to.
	SourceLightningMaps Source = 2
)

// String returns the network name, or "unknown" for a source this build does not
// recognise. An unrecognised source is passed through rather than dropped: the
// upstream is free to add networks, and losing their strokes would be worse than
// reporting an unfamiliar name.
func (s Source) String() string {
	switch s {
	case SourceBlitzortung:
		return "blitzortung.org"
	case SourceLightningMaps:
		return "lightningmaps.org"
	default:
		return "unknown"
	}
}

// Stroke is one located lightning discharge.
//
// Every stroke carries a Source and a StrokeID; together they form the
// identity, because the two networks issue independent id sequences and the
// same id from different networks means different things.
//
// The optional fields are pointers because "the upstream did not report this"
// and "the upstream reported zero" are different facts. Treating an absent
// deviation as zero would make the feed look falsely certain about stroke
// locations, which matters because location uncertainty is what decides whether
// a stroke is inside the region.
type Stroke struct {
	Src      Source
	StrokeID int64

	// Time is when the discharge occurred, as an absolute instant in UTC.
	Time time.Time

	Lat float64
	Lon float64

	// DeviationM is the upstream's estimate of location uncertainty in metres.
	DeviationM *int

	// DelayMS is how long the network took to locate this stroke, in
	// milliseconds.
	DelayMS *int

	// ServingBackend is the upstream's internal backend identifier. It exists
	// for diagnostics and for the id-space rule below.
	ServingBackend *int
}

// Key returns the stroke's identity: source and id combined.
//
// This is the deduplication key everywhere in the pipeline. It must be used
// rather than id alone, because two networks can issue the same id for
// different strokes.
func (s Stroke) Key() string {
	return fmt.Sprintf("%d:%d", int(s.Src), s.StrokeID)
}

// Network returns the reporting network's name.
func (s Stroke) Network() string {
	return s.Src.String()
}

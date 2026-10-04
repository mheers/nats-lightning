package feed_test

import (
	"testing"

	"github.com/heers-it/lightningfeed/internal/feed"
	"github.com/heers-it/lightningfeed/internal/geo"
	"github.com/heers-it/lightningfeed/internal/testsupport/natsd"
)

// feedPublisher starts a publisher against the test broker, optionally adjusted.
func feedPublisher(t *testing.T, ns *natsd.Server, mutate ...func(*feed.Options)) *feed.Publisher {
	t.Helper()
	nc := ns.Connect(t)

	opts := feed.Options{NATS: nc, Stream: "LIGHTNING"}
	for _, m := range mutate {
		m(&opts)
	}
	p, err := feed.NewPublisher(opts)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	return p
}

// streamMessageCount reports how many messages the stream holds.
func streamMessageCount(t *testing.T, ns *natsd.Server) uint64 {
	t.Helper()
	return ns.StreamMessageCount(t, "LIGHTNING")
}

// The configured region.
var testRegion = geo.Circle{Lat: 35.3340688, Lon: 24.4944483, RadiusKm: 10}

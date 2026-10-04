package feed_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/heers-it/lightningfeed/internal/feed"
	"github.com/heers-it/lightningfeed/internal/model"
	"github.com/heers-it/lightningfeed/internal/testsupport/natsd"
)

// Slice 9: publishing strokes to NATS.
//
// The seam is a real broker. A real nats-server runs on a random port inside the
// test process with file-backed JetStream storage, because the behaviours worth
// proving here - that a subscriber on a cell subject actually receives the
// stroke, and that JetStream's duplicate window really does swallow a replay -
// cannot be shown against a fake publisher. A fake would only prove the fake
// agrees with itself.

func TestPublishReachesASubscriberOnTheCellSubject(t *testing.T) {
	ns := natsd.Start(t)
	pub := feedPublisher(t, ns)

	nc := ns.Connect(t)
	defer nc.Close()

	// Subscribe before publishing so nothing is missed on a fast path.
	sub := subscribe(t, nc, "lightning.v1.>")
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	s := testStroke(1949808, 35.3340688, 24.4944483, 2211)
	if err := pub.Publish(context.Background(), s); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no message received: %v", err)
	}

	ev := decodeEvent(t, msg.Data)
	if ev.ID != fmt.Sprintf("%d:%d", int(s.Src), s.StrokeID) {
		t.Errorf("event id = %q, want %q", ev.ID, s.Key())
	}
	if ev.Type != "org.blitzortung.lightning.stroke.v1" {
		t.Errorf("event type = %q", ev.Type)
	}
	if ev.SpecVersion != "1.0" {
		t.Errorf("specversion = %q, want 1.0", ev.SpecVersion)
	}

	if ev.Data.Lat != s.Lat || ev.Data.Lon != s.Lon {
		t.Errorf("coordinates = %v,%v want %v,%v", ev.Data.Lat, ev.Data.Lon, s.Lat, s.Lon)
	}
	if ev.Data.DeviationM == nil || *ev.Data.DeviationM != 2211 {
		t.Errorf("deviation = %v, want 2211", ev.Data.DeviationM)
	}

	// The subject must be the one a region subscriber would use.
	if got, want := msg.Subject, "lightning.v1.src.2.cell.sw33j"; got != want {
		t.Errorf("subject = %q, want %q", got, want)
	}
}

// The upstream replays five minutes of history on every reconnect, so the same
// stroke is published repeatedly. JetStream's duplicate window is what stops a
// replay reaching stream consumers.
//
// The window applies to the stream, not to the subject. A plain Core NATS
// subscription sees every publish including duplicates, which is why consumers of
// this feed must read through a JetStream consumer. Both surfaces are asserted
// here so that distinction cannot regress unnoticed.
func TestTheBrokerSuppressesAReplayedPublish(t *testing.T) {
	ns := natsd.Start(t)
	pub := feedPublisher(t, ns)
	nc := ns.Connect(t)

	core := subscribe(t, nc, "lightning.v1.>")

	// A JetStream consumer is the surface dedup is supposed to protect.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	ci, err := js.AddConsumer("LIGHTNING", &nats.ConsumerConfig{
		Durable:   "replay-test",
		AckPolicy: nats.AckNonePolicy,
		// A pull consumer has no deliver subject, so ask for a push consumer.
		DeliverSubject: "lf.test.deliver",
		FilterSubject:  "lightning.v1.>",
	})
	if err != nil {
		t.Fatalf("adding consumer: %v", err)
	}
	consumer := subscribe(t, nc, ci.Config.DeliverSubject)

	s := testStroke(1949808, 35.3340688, 24.4944483, 2211)

	// Publish the same stroke five times, as a reconnect would.
	for i := range 5 {
		if err := pub.Publish(context.Background(), s); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// The stream keeps exactly one copy.
	if n := streamMessageCount(t, ns); n != 1 {
		t.Errorf("stream holds %d messages after five identical publishes, want 1", n)
	}

	// A stream consumer sees exactly one copy.
	if _, err := consumer.NextMsg(3 * time.Second); err != nil {
		t.Fatalf("stream consumer received nothing: %v", err)
	}
	if _, err := consumer.NextMsg(300 * time.Millisecond); err == nil {
		t.Error("a stream consumer received the replayed stroke; the duplicate window is not working")
	}

	// Core subscribers are not protected, and that is the broker's contract
	// rather than a defect here. Documenting it so nobody relies on it.
	coreCount := 0
	for {
		if _, err := core.NextMsg(100 * time.Millisecond); err != nil {
			break
		}
		coreCount++
	}
	if coreCount == 0 {
		t.Error("expected a Core subscriber to see the publishes at all")
	}
	t.Logf("Core subscriber saw %d of 5 publishes; stream consumers see 1. "+
		"Consumers must therefore use a JetStream consumer, not a Core subscription", coreCount)
}

// Two networks report the same id. They are different strokes and both must
// survive, which is why the message id carries the source.
func TestTheSameIDFromTwoNetworksIsTwoMessages(t *testing.T) {
	ns := natsd.Start(t)
	pub := feedPublisher(t, ns)

	nc := ns.Connect(t)
	defer nc.Close()
	sub := subscribe(t, nc, "lightning.v1.>")
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	a := testStroke(42, 35.4, 24.5, 1000)
	b := testStroke(42, 35.4, 24.5, 1000)
	b.Src = model.SourceBlitzortung

	for _, s := range []model.Stroke{a, b} {
		if err := pub.Publish(context.Background(), s); err != nil {
			t.Fatalf("Publish(%v): %v", s.Key(), err)
		}
	}

	if n := streamMessageCount(t, ns); n != 2 {
		t.Errorf("stream holds %d messages, want 2 (one per network)", n)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Errorf("first message: %v", err)
	}
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Errorf("second message: %v", err)
	}
}

// A configured region must have its judgement baked into the event so consumers
// do not re-implement haversine or guess at the uncertainty band.
func TestEventsCarryTheRegionJudgementWhenARegionIsConfigured(t *testing.T) {
	ns := natsd.Start(t)
	pub := feedPublisher(t, ns, func(o *feed.Options) {
		o.Region = testRegion
	})

	nc := ns.Connect(t)
	defer nc.Close()
	sub := subscribe(t, nc, "lightning.v1.>")
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Three strokes: certainly in, boundary, certainly out.
	in := testStroke(1, testRegion.Lat, testRegion.Lon, 500)
	boundary := testStroke(2, offsetNorth(testRegion.Lat, 9), testRegion.Lon, 2000)
	outside := testStroke(3, offsetNorth(testRegion.Lat, 20), testRegion.Lon, 1000)

	for _, s := range []model.Stroke{in, boundary, outside} {
		if err := pub.Publish(context.Background(), s); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Keyed by stroke id: all three come from the same network, so keying by
	// network would collapse them into one and hide the result.
	got := map[int64]event{}
	for range 3 {
		msg, err := sub.NextMsg(3 * time.Second)
		if err != nil {
			t.Fatalf("expected three events, got: %v", err)
		}
		ev := decodeEvent(t, msg.Data)
		if ev.Data.Certainty == "" {
			t.Fatalf("event %s has no certainty band", ev.ID)
		}
		if ev.Data.DistanceKm == nil {
			t.Fatalf("event %s has no distance", ev.ID)
		}
		got[ev.Data.ID] = ev
	}

	if len(got) != 3 {
		t.Fatalf("received %d distinct strokes, want 3", len(got))
	}
	for _, tc := range []struct {
		id   int64
		want string
	}{
		{in.StrokeID, "in"},
		{boundary.StrokeID, "boundary"},
		{outside.StrokeID, "out"},
	} {
		ev, ok := got[tc.id]
		if !ok {
			t.Errorf("stroke %d never arrived", tc.id)
			continue
		}
		if string(ev.Data.Certainty) != tc.want {
			t.Errorf("stroke %d certainty = %q, want %q", tc.id, ev.Data.Certainty, tc.want)
		}
	}

	// The boundary stroke counts as inside, per the project decision, so it
	// must be published rather than filtered out, yet stay distinguishable
	// from a certain one.
	if _, published := got[boundary.StrokeID]; !published {
		t.Error("the boundary stroke was filtered out; the decided policy is to include it")
	}
}

// Attribution must travel with every message. The upstream's terms require the
// source of the data to be clearly identified, and an in-band field survives
// consumers that never read the README.
func TestEventsCarryAttribution(t *testing.T) {
	ns := natsd.Start(t)
	pub := feedPublisher(t, ns)

	nc := ns.Connect(t)
	defer nc.Close()
	sub := subscribe(t, nc, "lightning.v1.>")
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := pub.Publish(context.Background(), testStroke(7, 35.4, 24.5, 900)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no message: %v", err)
	}

	ev := decodeEvent(t, msg.Data)
	if ev.Data.Attribution == "" {
		t.Fatal("event has no attribution")
	}
	for _, want := range []string{"Blitzortung", "CC BY-SA"} {
		if !strings.Contains(ev.Data.Attribution, want) {
			t.Errorf("attribution %q does not mention %q", ev.Data.Attribution, want)
		}
	}
}

// Without a region configured the feed is world-wide and must not invent
// distances or certainty bands it cannot justify.
func TestEventsOmitRegionJudgementWhenNoRegionIsConfigured(t *testing.T) {
	ns := natsd.Start(t)
	pub := feedPublisher(t, ns) // no region

	nc := ns.Connect(t)
	defer nc.Close()
	sub := subscribe(t, nc, "lightning.v1.>")
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if err := pub.Publish(context.Background(), testStroke(7, 35.4, 24.5, 900)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no message: %v", err)
	}

	ev := decodeEvent(t, msg.Data)
	if ev.Data.Certainty != "" {
		t.Errorf("certainty = %q, want empty when no region is configured", ev.Data.Certainty)
	}
	if ev.Data.DistanceKm != nil {
		t.Errorf("distance = %v, want nil when no region is configured", *ev.Data.DistanceKm)
	}
}

// Subjects are built from the stroke's own coordinates, so a consumer that
// recomputes them must get the same answer.
func TestSubjectMatchesTheStrokeLocation(t *testing.T) {
	ns := natsd.Start(t)
	pub := feedPublisher(t, ns)

	nc := ns.Connect(t)
	defer nc.Close()
	sub := subscribe(t, nc, "lightning.v1.>")
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	cases := []struct {
		lat, lon float64
		wantCell string
	}{
		{35.3340688, 24.4944483, "sw33j"},
		{41.39, 2.15, "sp3e2"},
		{35.593153, 26.354258, "sw6dr"},
	}
	for i, tc := range cases {
		s := testStroke(int64(1000+i), tc.lat, tc.lon, 1500)
		if err := pub.Publish(context.Background(), s); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		msg, err := sub.NextMsg(3 * time.Second)
		if err != nil {
			t.Fatalf("case %d: no message: %v", i, err)
		}
		want := fmt.Sprintf("lightning.v1.src.2.cell.%s", tc.wantCell)
		if msg.Subject != want {
			t.Errorf("case %d: subject = %q, want %q", i, msg.Subject, want)
		}
	}
}

// --- helpers ---

// event mirrors the CloudEvent envelope the publisher emits.
type event struct {
	SpecVersion string `json:"specversion"`
	Type        string `json:"type"`
	Source      string `json:"source"`
	ID          string `json:"id"`
	Subject     string `json:"subject"`
	Time        string `json:"time"`
	Data        struct {
		ID          int64    `json:"id"`
		Src         int      `json:"src"`
		Network     string   `json:"network"`
		Time        string   `json:"time"`
		Lat         float64  `json:"lat"`
		Lon         float64  `json:"lon"`
		DeviationM  *int     `json:"deviation_m"`
		DelayMS     *int     `json:"delay_ms"`
		DistanceKm  *float64 `json:"distance_km"`
		Certainty   string   `json:"certainty"`
		Attribution string   `json:"attribution"`
	} `json:"data"`
}

func decodeEvent(t *testing.T, raw []byte) event {
	t.Helper()
	var ev event
	if err := json.Unmarshal(raw, &ev); err != nil {
		t.Fatalf("event is not valid JSON: %v\n%s", err, raw)
	}
	return ev
}

func testStroke(id int64, lat, lon float64, dev int) model.Stroke {
	delay := 1800
	srv := 1
	return model.Stroke{
		Src:            model.SourceLightningMaps,
		StrokeID:       id,
		Time:           time.Now().UTC(),
		Lat:            lat,
		Lon:            lon,
		DeviationM:     &dev,
		DelayMS:        &delay,
		ServingBackend: &srv,
	}
}

func offsetNorth(lat, km float64) float64 {
	const kmPerDegLat = 111.32
	return lat + km/kmPerDegLat
}

// subscribe opens a subscription and waits for the server to register it, so a
// publish immediately afterwards cannot race the subscription.
func subscribe(t *testing.T, nc *nats.Conn, subject string) *nats.Subscription {
	t.Helper()
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("subscribing to %s: %v", subject, err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return sub
}

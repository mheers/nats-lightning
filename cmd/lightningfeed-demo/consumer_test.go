package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/mheers/nats-lightning/internal/feed"
	"github.com/mheers/nats-lightning/internal/geo"
	"github.com/mheers/nats-lightning/internal/model"
	"github.com/mheers/nats-lightning/internal/testsupport/natsd"
)

// This exercises the demo the way a reader runs it: a real broker, the real
// publisher, the real subjects, and the real CloudEvent envelope. A demo tested
// only against events it built itself would agree with a broken bridge.
//
// The bridge itself is not run. Running it would mean opening a connection to
// the live upstream, which throttles new connections severely enough that a test
// suite would consume everyone else's share.

func TestDemoReadsWhatTheBridgePublishes(t *testing.T) {
	srv := natsd.Start(t)
	cfg := regionConfig(t, roussospiti, 4)

	subjects, err := subjectsFor(cfg)
	if err != nil {
		t.Fatalf("subjectsFor: %v", err)
	}
	cells, err := cfg.Cells()
	if err != nil {
		t.Fatalf("cells: %v", err)
	}

	nc, err := nats.Connect(srv.URL)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	pub, err := feed.NewPublisher(feed.Options{
		NATS:   nc,
		Stream: cfg.Stream,
		Cells:  cells,
		Region: cfg.Region,
	})
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}

	out := captureFile(t)
	r := &reader{
		out:       out,
		view:      options{view: "log", durable: "demo-test", fade: time.Minute},
		region:    cfg.Region,
		startedAt: time.Now(),
		byBand:    map[geo.Certainty]int{},
		byNet:     map[string]int{},
		closestKm: math.Inf(1),
	}

	if _, err := js.Subscribe("",
		r.handle,
		nats.Durable("demo-test"),
		nats.BindStream(cfg.Stream),
		nats.ConsumerFilterSubjects(subjects...),
		nats.DeliverAll(),
		nats.AckExplicit(),
	); err != nil {
		t.Fatalf("subscribing: %v", err)
	}

	// Four messages: two strokes inside the radius, one just beyond it that the
	// bridge published anyway because its cell overlaps the region, and one that
	// is not a stroke at all. The demo has to tell all four apart.
	publishTestStrokes(t, pub, js, cells)

	const want = 4
	waitFor(t, 5*time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.inside+r.filtered+r.undecod >= want
	})

	r.mu.Lock()
	inside, filtered, undecod := r.inside, r.filtered, r.undecod
	closest, haveClosest := r.closestKm, r.haveClosest
	networks := len(r.byNet)
	r.mu.Unlock()

	if inside != 2 {
		t.Errorf("counted %d strokes inside a %v km region, want 2", inside, cfg.Region.RadiusKm)
	}
	if filtered != 1 {
		t.Errorf("counted %d strokes beyond the radius as published, want 1", filtered)
	}
	if undecod != 1 {
		t.Errorf("counted %d unreadable messages, want 1", undecod)
	}
	if !haveClosest || closest > cfg.Region.RadiusKm {
		t.Errorf("closest stroke reported as %v km (present %v), want inside a %v km region",
			closest, haveClosest, cfg.Region.RadiusKm)
	}
	if networks != 1 {
		t.Errorf("saw %d networks, want 1", networks)
	}
}

// A payload whose certainty band contradicts its own coordinates must not be
// believed. The bridge computes the band correctly, so this cannot happen in
// production — but the demo is the thing people copy, and a consumer that trusts
// a verdict it could check itself has no way to notice when that verdict is
// wrong. The count would then disagree with the membership test that produced it,
// and the two numbers would not add up.
func TestDemoClassifiesForItselfRatherThanBelievingThePayload(t *testing.T) {
	srv := natsd.Start(t)
	cfg := regionConfig(t, roussospiti, 4)

	subjects, err := subjectsFor(cfg)
	if err != nil {
		t.Fatalf("subjectsFor: %v", err)
	}

	nc, err := nats.Connect(srv.URL)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	// The stream exists because the bridge created it, which is the situation a
	// consumer actually joins: an existing stream, not one it made for itself.
	// Creating it here through the publisher is how that gets reproduced without a
	// second implementation of the stream's configuration.
	cells, err := cfg.Cells()
	if err != nil {
		t.Fatalf("cells: %v", err)
	}
	if _, err := feed.NewPublisher(feed.Options{
		NATS: nc, Stream: cfg.Stream, Cells: cells, Region: cfg.Region,
	}); err != nil {
		t.Fatalf("creating the stream: %v", err)
	}

	r := &reader{
		out:       captureFile(t),
		view:      options{view: "log", durable: "demo-liar", fade: time.Minute},
		region:    cfg.Region,
		startedAt: time.Now(),
		byBand:    map[geo.Certainty]int{},
		byNet:     map[string]int{},
		closestKm: math.Inf(1),
	}

	if _, err := js.Subscribe("",
		r.handle,
		nats.Durable("demo-liar"),
		nats.BindStream(cfg.Stream),
		nats.ConsumerFilterSubjects(subjects...),
		nats.DeliverAll(),
		nats.AckExplicit(),
	); err != nil {
		t.Fatalf("subscribing: %v", err)
	}

	// A stroke well outside the radius, on a subject the region publishes to, that
	// claims to be certainly inside.
	subject := subjects[0]
	lat := roussospiti.Lat + (roussospiti.RadiusKm*1.4)/kmPerDegLat
	now := time.Now().UTC().Format(time.RFC3339Nano)

	payload := fmt.Sprintf(`{"specversion":"1.0","type":"%s","source":"lightning.v1","id":"2:1",
	  "subject":%q,"time":%q,"datacontenttype":"application/json",
	  "data":{"id":1,"src":2,"network":"lightningmaps.org","time":%q,
	    "lat":%v,"lon":%v,"certainty":"in","received_at":%q,"attribution":%q}}`,
		feed.CloudEventType, subject, now, now, lat, roussospiti.Lon, now, feed.Attribution)

	if _, err := js.Publish(subject, []byte(payload)); err != nil {
		t.Fatalf("publishing: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.filtered+r.inside+r.undecod >= 1
	})

	r.mu.Lock()
	inside, filtered := r.inside, r.filtered
	inBand := r.byBand[geo.CertaintyIn]
	r.mu.Unlock()

	if inside != 0 {
		t.Errorf("counted %d strokes inside a %v km region, want 0", inside, cfg.Region.RadiusKm)
	}
	if filtered != 1 {
		t.Errorf("filtered %d strokes, want 1", filtered)
	}
	if inBand != 0 {
		t.Errorf("tallied %d strokes as certainly inside, taking the payload's word for it", inBand)
	}
}

// The durable must survive the demo exiting. nats.go deletes a consumer that the
// library created itself when the subscription is unsubscribed, and that applies
// to durables too — so an Unsubscribe anywhere in this command would throw away
// the position on every exit while looking, from the outside, exactly like a demo
// that had resumed. This asserts the two things that must both hold: the durable
// is still there, and it is filtered to the region's subjects.
func TestTheDurableSurvivesTheDemoExiting(t *testing.T) {
	srv := natsd.Start(t)
	cfg := regionConfig(t, roussospiti, 4)

	subjects, err := subjectsFor(cfg)
	if err != nil {
		t.Fatalf("subjectsFor: %v", err)
	}
	cells, err := cfg.Cells()
	if err != nil {
		t.Fatalf("cells: %v", err)
	}

	nc, err := nats.Connect(srv.URL)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	if _, err := feed.NewPublisher(feed.Options{
		NATS: nc, Stream: cfg.Stream, Cells: cells, Region: cfg.Region,
	}); err != nil {
		t.Fatalf("creating the stream: %v", err)
	}

	sub, err := js.Subscribe("",
		func(*nats.Msg) {},
		nats.Durable("survivor"),
		nats.BindStream(cfg.Stream),
		nats.ConsumerFilterSubjects(subjects...),
		nats.DeliverNew(),
		nats.AckExplicit(),
	)
	if err != nil {
		t.Fatalf("subscribing: %v", err)
	}

	// This is what run does on exit: the connection closes and nothing else.
	nc.Close()

	next, err := nats.Connect(srv.URL)
	if err != nil {
		t.Fatalf("reconnecting: %v", err)
	}
	defer next.Close()

	nextJS, err := next.JetStream()
	if err != nil {
		t.Fatalf("jetstream after reconnect: %v", err)
	}
	info, err := nextJS.ConsumerInfo(cfg.Stream, "survivor")
	if err != nil {
		t.Fatalf("the durable did not survive the exit: %v", err)
	}
	if len(info.Config.FilterSubjects) != len(subjects) {
		t.Errorf("durable is filtered to %d subject(s), want %d",
			len(info.Config.FilterSubjects), len(subjects))
	}
	_ = sub // the handle is unused: only the consumer matters here
}

// publishTestStrokes publishes two strokes inside the region, one beyond the
// radius that the region still publishes, and one undecodable message.
//
// The third is the interesting one. The bridge publishes every stroke in a cell
// that overlaps the region, so a stroke just past the radius is delivered rather
// than dropped, and it is the demo's own radius test that has to catch it.
// Filtering only on the published subject would count it.
func publishTestStrokes(t *testing.T, pub *feed.Publisher, js nats.JetStreamContext, cells []string) {
	t.Helper()
	ctx := context.Background()

	north := func(km float64) (float64, float64) {
		// A degree of latitude north of the centre: unambiguous about direction, and
		// short enough that the distances are the ones the test means.
		return roussospiti.Lat + km/kmPerDegLat, roussospiti.Lon
	}

	for i, km := range []float64{roussospiti.RadiusKm * 0.2, roussospiti.RadiusKm * 0.4} {
		lat, lon := north(km)
		stroke := model.Stroke{
			Src:      model.SourceLightningMaps,
			StrokeID: int64(i + 1),
			Time:     time.Now(),
			Lat:      lat,
			Lon:      lon,
		}
		if i == 1 {
			// The second is nearer the edge, so the closest-stroke figure has a
			// deviation to reason about rather than a certain one.
			dev := 500
			stroke.DeviationM = &dev
		}
		if err := pub.Publish(ctx, stroke); err != nil {
			t.Fatalf("publishing an inside stroke: %v", err)
		}
	}

	lat, lon := publishedPointOutside(t, cells)
	if err := pub.Publish(ctx, model.Stroke{
		Src:      model.SourceLightningMaps,
		StrokeID: 99,
		Time:     time.Now(),
		Lat:      lat,
		Lon:      lon,
	}); err != nil {
		t.Fatalf("publishing a stroke beyond the radius: %v", err)
	}

	// On a real subject, so the consumer's filter passes it and the failure is
	// the payload rather than the routing.
	cell, err := geo.Encode(roussospiti.Lat, roussospiti.Lon, feed.CellPrecision)
	if err != nil {
		t.Fatalf("encoding a cell: %v", err)
	}
	if _, err := js.Publish("lightning.v1.src.2.cell."+cell, []byte("{not json")); err != nil {
		t.Fatalf("publishing an undecodable message: %v", err)
	}
}

// publishedPointOutside finds a point beyond the radius whose cell the region
// publishes anyway.
//
// It searches rather than computing one, because whether a given cell straddles
// the circle's edge depends on where the circle falls relative to the geohash
// grid. Assuming it does would make the test a coin flip that passes for the
// wrong reason: a point the bridge refused to publish would leave the demo's
// filtered count at zero and the assertion would fail for the wrong reason, or
// worse, a stroke the publisher rejected would look like a working test.
func publishedPointOutside(t *testing.T, cells []string) (lat, lon float64) {
	t.Helper()

	inRegion := make(map[string]struct{}, len(cells))
	for _, c := range cells {
		inRegion[c] = struct{}{}
	}

	for step := 1.02; step <= 2.0; step += 0.02 {
		lat = roussospiti.Lat + (roussospiti.RadiusKm*step)/kmPerDegLat
		lon = roussospiti.Lon

		if geo.HaversineKm(roussospiti.Lat, roussospiti.Lon, lat, lon) <= roussospiti.RadiusKm {
			continue
		}
		cell, err := geo.Encode(lat, lon, feed.CellPrecision)
		if err != nil {
			t.Fatalf("encoding a cell: %v", err)
		}
		if _, ok := inRegion[cell]; ok {
			return lat, lon
		}
	}
	t.Fatal("no published cell reaches beyond the radius, so there is nothing to test")
	return 0, 0
}

// waitFor polls until cond holds or the deadline passes.
//
// Polling rather than a channel, because the messages arrive on nats.go's own
// dispatch goroutine and there is nothing to subscribe to; the counters are the
// thing being waited on.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", d)
}

// captureFile returns an *os.File standing in for stdout.
//
// The reader writes through *os.File rather than an io.Writer so that it can ask
// the file whether it is a terminal, which is what decides between the map and
// the log. A temporary file is therefore the honest substitute: like a pipe, it
// is not a character device.
func captureFile(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

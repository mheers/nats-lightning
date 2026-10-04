package upstream_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/heers-it/lightningfeed/internal/model"
	"github.com/heers-it/lightningfeed/internal/testsupport/fakews"
	"github.com/heers-it/lightningfeed/internal/upstream"
)

// Slice 8b: the source bitmask.
//
// The trap this guards against is specific and silent: a mask bit and the
// source code a stroke reports are different identifiers. Getting them confused
// makes the client subscribe to one network while believing it selected another,
// and nothing anywhere reports an error. It was found by an end-to-end run
// against the live upstream, where subscribing with "the LightningMaps mask"
// delivered src:1 strokes from Blitzortung.org.

func TestMaskValuesMatchWhatTheUpstreamObserved(t *testing.T) {
	// Literal values, observed from the live feed and from the settings the
	// website serves. Changing a constant here is a deliberate act.
	if upstream.MaskReserved != 1 {
		t.Errorf("MaskReserved = %d, want 1", upstream.MaskReserved)
	}
	if upstream.MaskBlitzortung != 2 {
		t.Errorf("MaskBlitzortung = %d, want 2", upstream.MaskBlitzortung)
	}
	if upstream.MaskLightningMaps != 4 {
		t.Errorf("MaskLightningMaps = %d, want 4", upstream.MaskLightningMaps)
	}
	if upstream.MaskTesting != 8 {
		t.Errorf("MaskTesting = %d, want 8", upstream.MaskTesting)
	}
	if upstream.DefaultSrcMask != upstream.MaskLightningMaps {
		t.Errorf("DefaultSrcMask = %d, want %d", upstream.DefaultSrcMask, upstream.MaskLightningMaps)
	}
}

// The whole point of the mask is that selecting a network yields that network's
// source codes. The fake encodes the upstream's bit-to-src mapping so this holds
// end to end: subscribe, then look at what actually arrives.
func TestSubscribingWithTheLightningMapsMaskYieldsLightningMapsStrokes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mask   upstream.SrcMask
		wantSr model.Source
		wantOK bool
	}{
		{"lightningmaps.org", upstream.MaskLightningMaps, model.SourceLightningMaps, true},
		{"blitzortung.org", upstream.MaskBlitzortung, model.SourceBlitzortung, true},
		{"reserved bit carries nothing", upstream.MaskReserved, model.Source(0), false},
		{"testing bit carries nothing", upstream.MaskTesting, model.Source(0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, ok := tc.mask.SrcForMask()
			if ok != tc.wantOK {
				t.Fatalf("SrcForMask() ok = %v, want %v", ok, tc.wantOK)
			}
			if src != tc.wantSr {
				t.Fatalf("SrcForMask() = %v, want %v", src, tc.wantSr)
			}
		})
	}
}

// Selecting a network must produce strokes reporting that network, not another.
// The fake applies the upstream's own bit-to-src mapping, so a wrong mask here
// shows up as the wrong network in the delivered strokes.
func TestClientReceivesStrokesFromTheNetworkItSelected(t *testing.T) {
	for _, tc := range []struct {
		name string
		mask upstream.SrcMask
		want model.Source
	}{
		{"lightningmaps.org", upstream.MaskLightningMaps, model.SourceLightningMaps},
		{"blitzortung.org", upstream.MaskBlitzortung, model.SourceBlitzortung},
		{"both networks", upstream.MaskBlitzortung | upstream.MaskLightningMaps, 0}, // either
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakews.Start(t, &fakews.Server{
				HonourSourceMask: true,
				Frames: []string{
					strokeFrameWithMask(tc.mask),
				},
				FrameDelay:       2 * time.Millisecond,
				HeartbeatForever: true,
			})

			c := newTestClient(t, srv.URL(), func(o *upstream.Options) {
				o.SourceMask = tc.mask
			})
			got := &collector{}

			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			_ = c.Run(ctx, got.sink)

			strokes := got.waitFor(t, 1, 2*time.Second)
			for _, s := range strokes {
				if tc.want == 0 {
					// Either network is acceptable when both were requested.
					if s.Src != model.SourceBlitzortung && s.Src != model.SourceLightningMaps {
						t.Errorf("stroke src = %v, want one of the two selected networks", int(s.Src))
					}
					continue
				}
				if s.Src != tc.want {
					t.Errorf("stroke src = %v, want %v: the mask selected the wrong network",
						int(s.Src), int(tc.want))
				}
			}
		})
	}
}

// A mask that selects nothing must yield no strokes at all, rather than
// everything or an error.
func TestClientReceivesNothingWhenNoNetworkIsSelected(t *testing.T) {
	srv := fakews.Start(t, &fakews.Server{
		HonourSourceMask: true,
		Frames: []string{
			strokeFrameWithMask(upstream.MaskReserved),
		},
		FrameDelay:       2 * time.Millisecond,
		HeartbeatForever: true,
	})

	c := newTestClient(t, srv.URL(), func(o *upstream.Options) {
		o.SourceMask = upstream.MaskReserved
	})
	got := &collector{}

	ctx, cancel := context.WithTimeout(t.Context(), 600*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx, got.sink)

	if n := len(got.snapshot()); n != 0 {
		t.Errorf("got %d strokes for a mask that selects no network, want 0", n)
	}
}

// strokeFrameWithMask builds a frame the fake will accept for that mask: it
// reports one stroke per selected network, with the source code that network
// actually uses on the wire.
func strokeFrameWithMask(mask upstream.SrcMask) string {
	var strokes []string
	for _, bit := range []upstream.SrcMask{
		upstream.MaskReserved,
		upstream.MaskBlitzortung,
		upstream.MaskLightningMaps,
		upstream.MaskTesting,
	} {
		if mask&bit == 0 {
			continue
		}
		src, ok := bit.SrcForMask()
		if !ok {
			continue // reserved and testing carry nothing
		}
		strokes = append(strokes, oneStrokeForSrc(int(src), "35.5", "26.3"))
	}
	return `{"time":1791106720,"flags":{"2":0},"strokes":[` + joinComma(strokes) + `]}`
}

// oneStrokeForSrc is oneStroke with an explicit source code.
func oneStrokeForSrc(src int, lat, lon string) string {
	b, _ := json.Marshal(map[string]any{
		"time": 1791106639235, "lat": 35.593153, "lon": 26.354258,
		"src": src, "srv": 1, "id": 1940052, "del": 1711, "dev": 4556,
	})
	return string(b)
}

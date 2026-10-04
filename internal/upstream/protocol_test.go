package upstream

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/heers-it/lightningfeed/internal/model"
)

// Slice 7: ParseFrame turns one upstream WebSocket frame into strokes.
//
// The fixtures are real frames captured from
// wss://live.lightningmaps.org on 2026-10-04 with the subscribe frame pointed at
// the configured Roussospiti region. Testing against captured traffic rather
// than invented input is deliberate: it is the only way to notice that the feed
// sends frames this code was never written to expect.

const fixtureDir = "../../internal/testsupport/fixtures"

// loadFixture reads a captured frame from the golden file.
func loadFixture(t *testing.T, file, frame string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(fixtureDir, file))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	var capture struct {
		Frames []string `json:"frames"`
		Odd    []string `json:"odd"`
		Hello  struct {
			Cid  int     `json:"cid"`
			Con  int     `json:"con"`
			Port string  `json:"port"`
			Time float64 `json:"time"`
			K    float64 `json:"k"`
		} `json:"hello"`
	}
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("parsing fixture: %v", err)
	}

	switch {
	case frame == "hello":
		b, _ := json.Marshal(capture.Hello)
		return b
	case frame == "heartbeat":
		if len(capture.Odd) == 0 {
			t.Fatal("fixture has no heartbeat frames")
		}
		return []byte(capture.Odd[0])
	case frame == "0":
		if len(capture.Frames) == 0 {
			t.Fatal("fixture has no stroke frames")
		}
		return []byte(capture.Frames[0])
	}
	t.Fatalf("unknown fixture frame %q", frame)
	return nil
}

const liveFixture = "live_roussospiti_20261004.json"

func TestParsesARealStrokeFrameIntoStrokes(t *testing.T) {
	frame, err := ParseFrame(loadFixture(t, liveFixture, "0"))
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}

	if frame.Kind != FrameStrokes {
		t.Errorf("Kind = %v, want %v", frame.Kind, FrameStrokes)
	}
	if len(frame.Strokes) == 0 {
		t.Fatal("expected the captured frame to contain strokes")
	}

	first := frame.Strokes[0]

	// Values copied from the captured frame:
	// {"time":1791106639235,"lat":35.593153,"lon":26.354258,"src":2,
	//  "srv":1,"id":1940052,"del":1711,"dev":4556}
	if want := int64(1940052); first.StrokeID != want {
		t.Errorf("StrokeID = %d, want %d", first.StrokeID, want)
	}
	if want := model.Source(2); first.Src != want {
		t.Errorf("Src = %d, want %d", first.Src, want)
	}
	if first.Lat < 35.593152 || first.Lat > 35.593154 {
		t.Errorf("Lat = %v, want 35.593153", first.Lat)
	}
	if first.Lon < 26.354257 || first.Lon > 26.354259 {
		t.Errorf("Lon = %v, want 26.354258", first.Lon)
	}
	if first.DeviationM == nil || *first.DeviationM != 4556 {
		t.Errorf("DeviationM = %v, want 4556", first.DeviationM)
	}
	if first.DelayMS == nil || *first.DelayMS != 1711 {
		t.Errorf("DelayMS = %v, want 1711", first.DelayMS)
	}
	if first.ServingBackend == nil || *first.ServingBackend != 1 {
		t.Errorf("ServingBackend = %v, want 1", first.ServingBackend)
	}

	// The upstream sends stroke time as Unix milliseconds; it must become an
	// absolute instant, not a duration or a millisecond count.
	wantTime := time.UnixMilli(1791106639235).UTC()
	if !first.Time.Equal(wantTime) {
		t.Errorf("Time = %v, want %v", first.Time.UTC(), wantTime)
	}
	if first.Time.Location() != time.UTC {
		t.Errorf("Time location = %v, want UTC", first.Time.Location())
	}
}

// The upstream keeps the connection warm with a bare time frame roughly every
// ten seconds. A decoder that treats a missing "strokes" key as a protocol
// violation would drop the connection ten times a minute.
func TestParsesHeartbeatFramesWithoutStrokes(t *testing.T) {
	frame, err := ParseFrame(loadFixture(t, liveFixture, "heartbeat"))
	if err != nil {
		t.Fatalf("ParseFrame on a heartbeat: %v", err)
	}
	if frame.Kind != FrameHeartbeat {
		t.Errorf("Kind = %v, want %v", frame.Kind, FrameHeartbeat)
	}
	if len(frame.Strokes) != 0 {
		t.Errorf("got %d strokes from a heartbeat, want 0", len(frame.Strokes))
	}
	if frame.ServerTime == 0 {
		t.Error("expected the heartbeat's server time to be recorded")
	}
}

func TestRecognisesTheHelloFrame(t *testing.T) {
	// {"cid":33821,"con":90,"port":"8081","time":1791106720.333,"k":798253216.2331275}
	frame, err := ParseFrame(loadFixture(t, liveFixture, "hello"))
	if err != nil {
		t.Fatalf("ParseFrame on the hello: %v", err)
	}
	if frame.Kind != FrameHello {
		t.Fatalf("Kind = %v, want %v", frame.Kind, FrameHello)
	}
	if frame.Hello.ClientID != 33821 {
		t.Errorf("ClientID = %d, want 33821", frame.Hello.ClientID)
	}
	if frame.Hello.Connected != 90 {
		t.Errorf("Connected = %d, want 90", frame.Hello.Connected)
	}
	if frame.Hello.BackendPort != "8081" {
		t.Errorf("BackendPort = %q, want 8081", frame.Hello.BackendPort)
	}
}

// The hello's keepalive token is deliberately not answered. Verified by A/B
// test against the live feed: the server streams identically when the client
// ignores it, replies with garbage, or replies correctly.
func TestHelloExposesButDoesNotRequireTheKeepaliveToken(t *testing.T) {
	frame, err := ParseFrame(loadFixture(t, liveFixture, "hello"))
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if frame.Hello.Keepalive == 0 {
		t.Error("expected the keepalive token to be recorded for diagnostics")
	}
}

// The feed is undocumented and can gain fields at any time. A new field must not
// break decoding, or an upstream change would silently stop the pipeline.
func TestIgnoresFieldsItDoesNotKnowAbout(t *testing.T) {
	raw := []byte(`{"time":1791106720,"flags":{"2":0},"strokes":[
		{"time":1791106639235,"lat":35.5,"lon":26.3,"src":2,"srv":1,
		 "id":1940052,"del":1711,"dev":4556,
		 "polarity":1,"energy_joules":42000,"cloud_to_ground":true,
		 "some_future_field":{"nested":[1,2,3]}},
		{"time":1791106639240,"lat":35.6,"lon":26.4,"src":2,"id":1940053}
	]}`)

	frame, err := ParseFrame(raw)
	if err != nil {
		t.Fatalf("ParseFrame with unknown fields: %v", err)
	}
	if len(frame.Strokes) != 2 {
		t.Fatalf("got %d strokes, want 2", len(frame.Strokes))
	}
}

// Optional fields are frequently absent. A stroke missing its deviation must
// still be delivered, with the uncertainty recorded as unknown rather than as a
// confident zero.
func TestStrokesWithoutOptionalFieldsAreStillUsable(t *testing.T) {
	raw := []byte(`{"time":1791106720,"strokes":[
		{"time":1791106639235,"lat":35.5,"lon":26.3,"src":1,"id":624207}
	]}`)

	frame, err := ParseFrame(raw)
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if len(frame.Strokes) != 1 {
		t.Fatalf("got %d strokes, want 1", len(frame.Strokes))
	}

	s := frame.Strokes[0]
	if s.DeviationM != nil {
		t.Errorf("DeviationM = %v, want nil when the upstream omitted it", *s.DeviationM)
	}
	if s.DelayMS != nil {
		t.Errorf("DelayMS = %v, want nil", *s.DelayMS)
	}
	if s.ServingBackend != nil {
		t.Errorf("ServingBackend = %v, want nil", *s.ServingBackend)
	}
	// The essential fields must still be there.
	if s.StrokeID == 0 || s.Lat == 0 || s.Lon == 0 || s.Time.IsZero() {
		t.Errorf("essential fields were lost: %+v", s)
	}
}

// The HTTP fallback endpoint encodes coordinates as JSON strings while the
// WebSocket encodes them as numbers. Supporting both costs little and removes a
// whole class of silent zero-coordinate bugs if the fallback is ever used.
func TestAcceptsCoordinatesEncodedAsStrings(t *testing.T) {
	raw := []byte(`{"time":1791106720,"strokes":[
		{"time":1791106639235,"lat":"40.617910","lon":"3.509261","src":2,"id":1940052,"del":1813,"dev":629}
	]}`)

	frame, err := ParseFrame(raw)
	if err != nil {
		t.Fatalf("ParseFrame with string coordinates: %v", err)
	}
	s := frame.Strokes[0]
	if s.Lat < 40.617909 || s.Lat > 40.617911 {
		t.Errorf("Lat = %v, want 40.617910", s.Lat)
	}
	if s.Lon < 3.509260 || s.Lon > 3.509262 {
		t.Errorf("Lon = %v, want 3.509261", s.Lon)
	}
}

// One malformed frame must never take down the connection. Rejecting only the
// bad frame is the difference between a glitch and an outage.
func TestRejectsMalformedFramesWithoutLosingTheConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"not json", `this is not json`},
		{"truncated object", `{"time":1791106720,"strokes":[`},
		{"strokes is not a list", `{"time":1791106720,"strokes":"nope"}`},
		{"empty payload", ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseFrame([]byte(tc.raw)); err == nil {
				t.Errorf("ParseFrame(%q) = nil error, want error", tc.raw)
			}
		})
	}
}

// A coordinate outside the valid range cannot be located on earth. Accepting it
// would place the stroke nowhere, and it would be counted as outside the region
// rather than reported as bad data.
func TestRejectsImpossibleCoordinates(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{"latitude past the pole", `{"strokes":[{"time":1,"lat":95.0,"lon":0,"src":2,"id":1}]}`},
		{"latitude below the south pole", `{"strokes":[{"time":1,"lat":-91.0,"lon":0,"src":2,"id":1}]}`},
		{"longitude past the dateline", `{"strokes":[{"time":1,"lat":0,"lon":181.0,"src":2,"id":1}]}`},
		{"latitude is text", `{"strokes":[{"time":1,"lat":"north","lon":0,"src":2,"id":1}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseFrame([]byte(tc.raw)); err == nil {
				t.Errorf("ParseFrame(%s) = nil error, want error", tc.raw)
			}
		})
	}
}

// The stroke identity is the pair (src, id): two networks issue independent id
// sequences, and the same id from different networks is a different stroke.
func TestStrokeIdentityCombinesSourceAndID(t *testing.T) {
	raw := []byte(`{"strokes":[
		{"time":1,"lat":35.5,"lon":26.3,"src":2,"id":1940052},
		{"time":2,"lat":35.5,"lon":26.3,"src":1,"id":1940052}
	]}`)

	frame, err := ParseFrame(raw)
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if frame.Strokes[0].Key() == frame.Strokes[1].Key() {
		t.Error("strokes from different sources share an identity")
	}
	if frame.Strokes[0].Key() != frame.Strokes[0].Key() {
		t.Error("Key is not stable across calls")
	}
}

// The network name travels with every stroke so consumers do not have to map
// numeric source codes themselves.
func TestStrokesCarryTheirNetworkName(t *testing.T) {
	raw := []byte(`{"strokes":[
		{"time":1,"lat":35.5,"lon":26.3,"src":1,"id":1},
		{"time":2,"lat":35.5,"lon":26.3,"src":2,"id":2},
		{"time":3,"lat":35.5,"lon":26.3,"src":9,"id":3}
	]}`)

	frame, err := ParseFrame(raw)
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}

	want := []string{"blitzortung.org", "lightningmaps.org", "unknown"}
	for i, w := range want {
		if got := frame.Strokes[i].Network(); got != w {
			t.Errorf("stroke %d network = %q, want %q", i, got, w)
		}
	}
}

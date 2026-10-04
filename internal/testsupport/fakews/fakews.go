// Package fakews provides a stand-in for the lightningmaps.org upstream.
//
// It is a real WebSocket server over a real TCP listener, not a mock of this
// project's own client. That distinction matters: the behaviours worth testing
// here live in the connection lifecycle and in how the client reacts to
// malformed or missing frames, and none of those can be exercised by
// substituting a method on the client.
//
// The server can be scripted to reproduce every failure mode measured against
// the live feed, including the ones the real upstream only exhibits occasionally.
package fakews

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Server is a scripted stand-in for the upstream feed.
type Server struct {
	// Hello is sent as the first frame. A nil Hello sends the real upstream's
	// opening shape.
	Hello []byte

	// NoHello suppresses the hello frame entirely, reproducing an upstream
	// that never answers the subscribe.
	NoHello bool

	// Frames are sent in order once the subscribe arrives. Each entry is sent
	// as one WebSocket message.
	Frames []string

	// FrameDelay is the pause between frames. Heartbeats arrive about every
	// ten seconds in production; tests use a much shorter value.
	FrameDelay time.Duration

	// CloseAfterFrames closes the connection once Frames have been sent,
	// reproducing an upstream that drops the socket.
	CloseAfterFrames bool

	// HangAfterFrames stops sending and never closes, reproducing the silent
	// connection that the client's idle watchdog has to catch.
	HangAfterFrames bool

	// HeartbeatForever keeps sending bare time frames every FrameDelay after
	// Frames are exhausted, and never closes.
	//
	// This is what the real upstream does: it holds the connection open with a
	// heartbeat roughly every ten seconds, so a healthy connection is not idle.
	// Tests that want to assert an exact stroke count need it, otherwise the
	// client's idle watchdog correctly reconnects and the script replays.
	HeartbeatForever bool

	// SubscribeFunc observes each subscribe frame the client sends.
	SubscribeFunc func(raw []byte)

	// HonourSourceMask makes the fake filter the scripted frames the way the
	// real upstream does, keeping only strokes whose source the client's
	// subscribe mask actually selected.
	//
	// This encodes an external contract: the upstream's mask bit for a network
	// is not the same number as the source code its strokes carry. A fake that
	// ignored the mask could not catch a client that selected the wrong
	// network, which is precisely the bug an end-to-end run found.
	HonourSourceMask bool

	http *httptest.Server

	subscribeMask int

	mu           sync.Mutex
	subscribes   [][]byte
	connections  int
	closedByPeer bool
}

// URL is the WebSocket endpoint to point a client at.
func (s *Server) URL() string {
	return "ws" + strings.TrimPrefix(s.http.URL, "http")
}

// Subscribes returns the raw subscribe frames received so far.
func (s *Server) Subscribes() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.subscribes))
	copy(out, s.subscribes)
	return out
}

// Connections returns how many connections have been accepted.
func (s *Server) Connections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connections
}

// Close indicates a client disconnected.
func (s *Server) ClosedByPeer() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closedByPeer
}

// Start launches the server and registers cleanup with t.
func Start(t *testing.T, s *Server) *Server {
	t.Helper()

	s.http = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(func() {
		s.http.Close()
		// Give in-flight handlers a moment to unwind before the test ends.
		time.Sleep(10 * time.Millisecond)
	})
	return s
}

// defaultHello matches the shape the real upstream opens with.
var defaultHello = []byte(
	`{"cid":33821,"con":90,"port":"8081","time":1791106720.333,"k":798253216.2331275}`)

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// The real upstream accepts any origin, so the fake must too or the
		// client would be built against a stricter contract.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()

	s.mu.Lock()
	s.connections++
	s.mu.Unlock()

	ctx := r.Context()

	// The client is expected to subscribe first. Read frames until we see one
	// that is not a keepalive reply, recording each.
	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			s.mu.Lock()
			s.closedByPeer = true
			s.mu.Unlock()
			return
		}
		if typ == websocket.MessageBinary {
			continue
		}
		raw := append([]byte(nil), data...)
		s.mu.Lock()
		s.subscribes = append(s.subscribes, raw)
		s.mu.Unlock()

		if s.SubscribeFunc != nil {
			s.SubscribeFunc(raw)
		}

		if s.HonourSourceMask {
			s.subscribeMask = parseSourceMask(raw)
		}
		break
	}

	if !s.NoHello {
		hello := s.Hello
		if hello == nil {
			hello = defaultHello
		}
		if err := conn.Write(ctx, websocket.MessageText, hello); err != nil {
			return
		}
	}

	for _, f := range s.Frames {
		if s.FrameDelay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.FrameDelay):
			}
		}
		out := f
		if s.HonourSourceMask {
			out = filterFrameByMask(f, s.subscribeMask)
		}
		if err := conn.Write(ctx, websocket.MessageText, []byte(out)); err != nil {
			return
		}
	}

	if s.HeartbeatForever {
		beat := []byte(`{"time":1791106736.835}`)
		for {
			if s.FrameDelay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(s.FrameDelay):
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Millisecond):
				}
			}
			if err := conn.Write(ctx, websocket.MessageText, beat); err != nil {
				return
			}
		}
	}

	switch {
	case s.CloseAfterFrames:
		conn.Close(websocket.StatusNormalClosure, "scripted close")
	case s.HangAfterFrames:
		// Hold the connection open and silent until the client gives up.
		<-r.Context().Done()
	}
}

// sourceMaskForSrc is the reverse of the upstream's mapping: which mask bit
// selects strokes carrying a given source code.
var sourceMaskForSrc = map[int]int{
	1: 2, // Blitzortung.org
	2: 4, // LightningMaps.org
}

// parseSourceMask reads the "a" field out of a subscribe frame.
func parseSourceMask(raw []byte) int {
	var frame struct {
		A int `json:"a"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return 0
	}
	return frame.A
}

// filterFrameByMask removes strokes the mask did not select, mirroring what the
// upstream does with the client's source selection.
func filterFrameByMask(frame string, mask int) string {
	var doc struct {
		Time    float64          `json:"time"`
		Flags   map[string]int   `json:"flags"`
		Strokes []map[string]any `json:"strokes"`
	}
	if err := json.Unmarshal([]byte(frame), &doc); err != nil {
		return frame
	}

	kept := make([]map[string]any, 0, len(doc.Strokes))
	for _, s := range doc.Strokes {
		src, _ := s["src"].(float64)
		want, known := sourceMaskForSrc[int(src)]
		if known && mask&want == 0 {
			continue // this network was not selected
		}
		kept = append(kept, s)
	}

	out, err := json.Marshal(struct {
		Time    float64          `json:"time"`
		Flags   map[string]int   `json:"flags"`
		Strokes []map[string]any `json:"strokes"`
	}{doc.Time, doc.Flags, kept})
	if err != nil {
		return frame
	}
	return string(out)
}

package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/mheers/nats-lightning/internal/model"
)

// Viewport is the geographic box the upstream is asked to stream.
//
// The upstream filters by viewport, but loosely: a 1.3 by 1.5 degree request was
// observed returning strokes spanning 7.7 by 7.0 degrees, with only about 14% of
// the live stream landing inside the requested box. Sending a tight viewport is
// therefore a courtesy that reduces load, never a correctness measure. Exact
// filtering is the consumer's job.
type Viewport struct {
	North float64
	East  float64
	South float64
	West  float64
}

// wholeWorld is the viewport used when no region is configured.
var wholeWorld = Viewport{North: 85, East: 179.9, South: -85, West: -179.9}

// Options configures a Client.
type Options struct {
	// URL is the WebSocket endpoint, for example
	// wss://live.lightningmaps.org:443/.
	URL string

	// SourceMask selects which networks to subscribe to.
	SourceMask SrcMask

	// Viewport bounds the region the upstream is asked for.
	Viewport Viewport

	// LastSeen maps a source to the highest stroke id already processed. It is
	// the upstream's resume hint and is safe to leave empty on a cold start.
	LastSeen map[model.Source]int64

	// HandshakeTimeout bounds the subscribe-and-hello exchange.
	HandshakeTimeout time.Duration

	// IdleTimeout forces a reconnect when no frame arrives for this long. It
	// needs to exceed the upstream's roughly ten second heartbeat.
	IdleTimeout time.Duration

	// ReconnectMin and ReconnectMax bound the retry delay.
	//
	// The floor is the important number. The upstream throttles new connections
	// hard: measured against the live service, attempts with no delay between
	// them succeeded 0 times out of 6, five seconds apart succeeded 2 of 6, and
	// fifteen seconds apart succeeded 6 of 6. Failures are silent — the TCP
	// connection hangs and never upgrades. A floor below fifteen seconds means
	// a reconnect loop that can never succeed.
	ReconnectMin time.Duration
	ReconnectMax time.Duration

	// OnSessionStart is called once per session, after the upstream's hello and
	// before any stroke is delivered.
	//
	// It exists for replay suppression: the upstream replays about five minutes
	// of history on every connection, so the pipeline has to open a backfill
	// window at exactly this moment. Wiring that here rather than in the caller
	// is what makes the layer work at all — it was previously defined on the
	// dedup filter and never called from anywhere in the pipeline.
	//
	// Nil means no callback.
	OnSessionStart func()

	// Logger receives diagnostics. Nil means discard.
	Logger *slog.Logger
}

// Defaults fills in unset options.
func (o Options) withDefaults() Options {
	if o.SourceMask == 0 {
		o.SourceMask = DefaultSrcMask
	}
	if o.Viewport == (Viewport{}) {
		o.Viewport = wholeWorld
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = 20 * time.Second
	}
	if o.IdleTimeout <= 0 {
		// Comfortably above the upstream's ten second heartbeat.
		o.IdleTimeout = 45 * time.Second
	}
	if o.ReconnectMin <= 0 {
		o.ReconnectMin = 15 * time.Second
	}
	if o.ReconnectMax <= 0 {
		o.ReconnectMax = 5 * time.Minute
	}
	if o.Logger == nil {
		o.Logger = slog.New(discardHandler{})
	}
	return o
}

// Sink receives each stroke as it is decoded. Returning an error stops the
// client, which is how backpressure reaches the rest of the pipeline.
type Sink func(ctx context.Context, stroke model.Stroke) error

// Client maintains a connection to the upstream and delivers strokes to a sink.
//
// It is safe for a single Run call at a time; the leader lock upstream of it
// guarantees only one instance is ever connected, because the upstream's
// throttling makes a second connection actively harmful.
type Client struct {
	opts Options

	reconnects atomic.Int64
	malformed  atomic.Int64
	rejected   atomic.Int64

	lastSeen    sync.Map // model.Source -> int64
	hello       atomic.Pointer[Hello]
	lastMessage atomic.Pointer[time.Time]

	// connected is the authoritative answer to "is there a live socket", and is
	// deliberately separate from lastMessage.
	//
	// Deriving it from lastMessage - as this once did - is wrong in exactly the
	// case that matters: lastMessage is only cleared once a dial succeeds, so
	// during a failed reconnect, which is this upstream's dominant failure mode
	// (silent hangs), the client kept reporting itself connected while it slept
	// through its backoff. A readiness probe built on that answer reports ready
	// during precisely the outage it exists to detect.
	connected atomic.Bool
}

// New returns a client. The connection is not made until Run is called.
func New(opts Options) *Client {
	c := &Client{opts: opts.withDefaults()}
	c.seedLastSeen(opts.LastSeen)
	return c
}

func (c *Client) seedLastSeen(m map[model.Source]int64) {
	if len(m) == 0 {
		return
	}
	for src, id := range m {
		c.lastSeen.Store(src, id)
	}
}

// Reconnects returns how many times the client has re-established a connection.
func (c *Client) Reconnects() int64 { return c.reconnects.Load() }

// MalformedFrames returns how many frames failed to decode entirely.
func (c *Client) MalformedFrames() int64 { return c.malformed.Load() }

// RejectedStrokes returns how many individual strokes were dropped because they
// could not be normalised, while the frames carrying them decoded fine.
//
// This is deliberately not folded into MalformedFrames. The two mean different
// things to an operator: a rising frame count points at the transport or the
// protocol, whereas a rising stroke count points at bad data inside otherwise
// healthy batches.
func (c *Client) RejectedStrokes() int64 { return c.rejected.Load() }

// Hello returns the upstream's greeting from the current or last connection.
func (c *Client) Hello() *Hello { return c.hello.Load() }

// LastSeen returns the highest stroke id recorded for a source, which is the
// cursor to persist so a restart can resume instead of replaying.
func (c *Client) LastSeen(src model.Source) int64 {
	if v, ok := c.lastSeen.Load(src); ok {
		return v.(int64)
	}
	return 0
}

// Connected reports whether the client currently holds a live upstream socket.
//
// It is false from the moment a session ends until the next handshake
// completes, which includes the whole of every reconnect backoff. It must not be
// derived from the last message time: the upstream's characteristic failure is a
// silent hang, so the gap between sessions is routinely minutes long and a stale
// frame would otherwise read as a healthy connection throughout it.
func (c *Client) Connected() bool { return c.connected.Load() }

// Run connects, streams strokes into sink, and reconnects until ctx ends.
//
// It returns nil or the context error on clean shutdown, the sink's error if the
// sink fails, and any error that makes reconnecting pointless.
func (c *Client) Run(ctx context.Context, sink Sink) error {
	for {
		started := time.Now()
		err := c.session(ctx, sink)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err == nil:
			return nil
		}

		// A sink failure is downstream of the feed; retrying would just hammer
		// whatever is already struggling.
		var se sinkError
		if errors.As(err, &se) {
			return se.err
		}

		// A session that ran long enough to be considered healthy resets the
		// backoff. Without this the counter is cumulative for the life of the
		// process, so a deployment that reconnects every few hours would drift
		// to the ceiling after a few days and then sit there permanently, even
		// though every one of those sessions was fine. Only a run of quick
		// failures should escalate the delay.
		if time.Since(started) >= healthySession {
			c.reconnects.Store(0)
		}

		delay := c.backoff()
		c.reconnects.Add(1)
		c.opts.Logger.Warn("upstream session ended, reconnecting",
			"error", err, "retry_in", delay, "url", c.opts.URL)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// sinkError distinguishes a sink failure from an upstream failure, so Run knows
// not to reconnect.
//
// It carries a single Unwrap rather than being wrapped with multiple %w verbs:
// errors.Unwrap only understands the single-error form, so a multi-%w chain
// would make the cause invisible and let Run exit reporting success.
type sinkError struct{ err error }

func (e sinkError) Error() string { return "sink: " + e.err.Error() }
func (e sinkError) Unwrap() error { return e.err }

// healthySession is how long a session must last to count as healthy and reset
// the backoff.
//
// It sits above the upstream's roughly ten second heartbeat by a wide margin,
// so a connection that received frames and then died is not mistaken for a
// healthy one, while a connection that worked for a few minutes plainly was.
const healthySession = 2 * time.Minute

// backoff returns the next retry delay, doubling from the floor to the ceiling
// with jitter. Jitter matters because several instances failing over together
// would otherwise reconnect in lockstep and hit the throttling together.
func (c *Client) backoff() time.Duration {
	n := c.reconnects.Load()
	delay := c.opts.ReconnectMin
	// n is the number of consecutive quick failures, and it is reset once a
	// session is healthy, so this loop is bounded in practice. The break keeps
	// it bounded by construction regardless.
	for range n {
		if delay >= c.opts.ReconnectMax {
			break
		}
		delay *= 2
	}
	if delay > c.opts.ReconnectMax {
		delay = c.opts.ReconnectMax
	}
	// +/- 25% jitter.
	jitter := 1 + (rand.Float64()*0.5 - 0.25)
	return time.Duration(float64(delay) * jitter)
}

// session runs one connection to exhaustion.
func (c *Client) session(ctx context.Context, sink Sink) error {
	// Clear the connection state before dialling rather than after a successful
	// dial. This is the whole point of tracking it separately: the upstream's
	// failure mode is a silent hang, so the time between a session ending and
	// the next one succeeding can be minutes, and the client must report itself
	// disconnected throughout.
	c.connected.Store(false)
	c.lastMessage.Store(nil)

	dialCtx, cancelDial := context.WithTimeout(ctx, c.opts.HandshakeTimeout)
	defer cancelDial()

	conn, _, err := websocket.Dial(dialCtx, c.opts.URL, nil)
	if err != nil {
		return fmt.Errorf("upstream: dialing: %w", err)
	}
	defer conn.CloseNow()

	conn.SetReadLimit(1 << 20) // 1 MiB; upstream batches top out around 60 KiB

	// Subscribe. The upstream requires at least "v" and "i"; the remaining
	// fields mirror what the website sends so the frame is indistinguishable
	// from a browser's.
	if err := c.subscribe(ctx, conn); err != nil {
		return fmt.Errorf("upstream: subscribing: %w", err)
	}

	handshakeCtx, cancelHandshake := context.WithTimeout(ctx, c.opts.HandshakeTimeout)
	defer cancelHandshake()

	if err := c.awaitHello(handshakeCtx, conn); err != nil {
		return err
	}

	// Only now is there a live socket. Before this point a dial that hangs or a
	// handshake that never completes must not read as connected.
	c.connected.Store(true)
	defer c.connected.Store(false)

	c.opts.Logger.Info("connected to upstream",
		"url", c.opts.URL, "hello", c.Hello(), "viewport", c.opts.Viewport)

	// The session is live, so a consumer that needs to suppress the replay the
	// upstream sends on every connect can open its window now. Doing it here
	// rather than at dial time is deliberate: the backfill that follows is
	// measured from the moment data starts arriving.
	if c.opts.OnSessionStart != nil {
		c.opts.OnSessionStart()
	}

	return c.readLoop(ctx, conn, sink)
}

// subscribe builds and sends the subscribe frame.
func (c *Client) subscribe(ctx context.Context, conn *websocket.Conn) error {
	lastSeen := map[string]int64{}
	c.lastSeen.Range(func(k, v any) bool {
		lastSeen[fmt.Sprintf("%d", k.(model.Source))] = v.(int64)
		return true
	})

	frame := map[string]any{
		"v": ProtocolVersion,
		"i": lastSeen,
		// Station maps inflate each stroke by roughly 36 times and are not
		// needed, so they stay off.
		"s":                      false,
		"a":                      int(c.opts.SourceMask),
		"z":                      12,
		"b":                      true,
		"h":                      "",
		"l":                      0,
		"t":                      0,
		"x":                      0,
		"w":                      0,
		"tx":                     0,
		"tw":                     1,
		"from_lightningmaps_org": true,
		"p": []float64{
			c.opts.Viewport.North,
			c.opts.Viewport.East,
			c.opts.Viewport.South,
			c.opts.Viewport.West,
		},
		"r": "A",
	}

	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, payload)
}

// awaitHello waits for the upstream's opening frame.
func (c *Client) awaitHello(ctx context.Context, conn *websocket.Conn) error {
	_, raw, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("upstream: awaiting hello: %w", err)
	}

	frame, err := ParseFrame(raw)
	if err != nil {
		return fmt.Errorf("upstream: decoding hello: %w", err)
	}
	if frame.Kind != FrameHello {
		return fmt.Errorf("upstream: expected a hello frame, got %q", frame.Kind)
	}

	h := frame.Hello
	c.hello.Store(&h)
	c.touch()

	// The hello carries a keepalive token. The website replies to it, but the
	// upstream streams identically whether the reply is correct, malformed, or
	// absent, so this client deliberately does not reproduce the transform.
	return nil
}

// readLoop consumes frames until the connection ends or the context is done.
func (c *Client) readLoop(ctx context.Context, conn *websocket.Conn, sink Sink) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		_, raw, err := c.readWithIdleWatchdog(ctx, conn)
		if err != nil {
			return err
		}

		frame, err := ParseFrame(raw)
		if err != nil {
			// The frame itself will not decode. One bad frame is a hiccup, not an
			// outage: log it, count it, and carry on reading.
			c.malformed.Add(1)
			c.opts.Logger.Warn("skipping undecodable frame",
				"error", err, "bytes", len(raw))
			continue
		}

		// The frame decoded but some strokes in it did not. Count them
		// separately from undecodable frames so a single bad coordinate in a
		// 500-stroke batch does not look like an upstream outage, and keep the
		// strokes that were fine.
		if frame.Rejected > 0 {
			c.rejected.Add(int64(frame.Rejected))
			c.opts.Logger.Warn("dropped unusable strokes from an otherwise good frame",
				"dropped", frame.Rejected,
				"kept", len(frame.Strokes),
				"reason", frame.RejectReason)
		}

		c.touch()

		if frame.Kind == FrameHello {
			// A second hello means the upstream recycled the session.
			h := frame.Hello
			c.hello.Store(&h)
			continue
		}
		if frame.Kind == FrameHeartbeat {
			continue
		}

		for _, stroke := range frame.Strokes {
			c.record(stroke)
			if err := sink(ctx, stroke); err != nil {
				return sinkError{err: err}
			}
		}
	}
}

// readWithIdleWatchdog reads one frame, failing if none arrives within
// IdleTimeout. A silent connection does not close on its own, so without this
// the client would sit there publishing nothing while looking healthy.
func (c *Client) readWithIdleWatchdog(ctx context.Context, conn *websocket.Conn) (websocket.MessageType, []byte, error) {
	type result struct {
		typ  websocket.MessageType
		data []byte
		err  error
	}

	ch := make(chan result, 1)
	go func() {
		typ, data, err := conn.Read(ctx)
		ch <- result{typ, data, err}
	}()

	select {
	case r := <-ch:
		return r.typ, r.data, r.err
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-time.After(c.opts.IdleTimeout):
		// Close abruptly rather than gracefully. A graceful close waits for the
		// peer's close frame, which a connection that has already gone silent
		// will never send; that turned a dead connection into a five second
		// stall before the reconnect could happen.
		conn.CloseNow()
		return 0, nil, fmt.Errorf("upstream: no frame within %v", c.opts.IdleTimeout)
	}
}

// touch records that a frame arrived.
func (c *Client) touch() {
	now := time.Now()
	c.lastMessage.Store(&now)
}

// record advances the resume cursor for a source.
//
// The cursor only ever moves forward. The upstream replays history on every
// connect, so a replayed stroke carries a lower id than the newest one already
// seen, and adopting it would rewind the cursor and make the next resume replay
// that window all over again.
func (c *Client) record(s model.Stroke) {
	for {
		prev := c.LastSeen(s.Src)
		if s.StrokeID <= prev {
			return
		}
		if _, loaded := c.lastSeen.LoadOrStore(s.Src, s.StrokeID); loaded {
			// Only this goroutine writes, so a re-read is not a race: it is the
			// same value unless a larger id has already been recorded, in which
			// case leaving it alone is the point.
			if c.LastSeen(s.Src) < s.StrokeID {
				c.lastSeen.Store(s.Src, s.StrokeID)
			}
			return
		}
		return
	}
}

// discardHandler drops log records when no logger is configured.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }

// LastMessage returns when the last frame arrived, or the zero time if the
// connection has not produced one.
//
// The health probe and the metrics both need this, and either one reaching into
// the client's own fields would be a coupling worth avoiding.
func (c *Client) LastMessage() time.Time {
	if t := c.lastMessage.Load(); t != nil {
		return *t
	}
	return time.Time{}
}

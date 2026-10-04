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

	"github.com/heers-it/lightningfeed/internal/model"
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

	reconnects  atomic.Int64
	malformed   atomic.Int64
	lastSeen    sync.Map // model.Source -> int64
	hello       atomic.Pointer[Hello]
	lastMessage atomic.Pointer[time.Time]
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

// MalformedFrames returns how many frames failed to decode.
func (c *Client) MalformedFrames() int64 { return c.malformed.Load() }

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

// Connected reports whether the client currently holds a live connection.
func (c *Client) Connected() bool { return c.lastMessage.Load() != nil }

// Run connects, streams strokes into sink, and reconnects until ctx ends.
//
// It returns nil or the context error on clean shutdown, the sink's error if the
// sink fails, and any error that makes reconnecting pointless.
func (c *Client) Run(ctx context.Context, sink Sink) error {
	for {
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

// backoff returns the next retry delay, doubling from the floor to the ceiling
// with jitter. Jitter matters because several instances failing over together
// would otherwise reconnect in lockstep and hit the throttling together.
func (c *Client) backoff() time.Duration {
	n := c.reconnects.Load()
	delay := c.opts.ReconnectMin
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
	dialCtx, cancelDial := context.WithTimeout(ctx, c.opts.HandshakeTimeout)
	defer cancelDial()

	conn, _, err := websocket.Dial(dialCtx, c.opts.URL, nil)
	if err != nil {
		return fmt.Errorf("upstream: dialing: %w", err)
	}
	defer conn.CloseNow()

	conn.SetReadLimit(1 << 20) // 1 MiB; upstream batches top out around 60 KiB
	c.lastMessage.Store(nil)

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

	c.opts.Logger.Info("connected to upstream",
		"url", c.opts.URL, "hello", c.Hello(), "viewport", c.opts.Viewport)

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
			// One bad frame is a hiccup, not an outage. Log it, count it, and
			// carry on reading.
			c.malformed.Add(1)
			c.opts.Logger.Warn("skipping undecodable frame",
				"error", err, "bytes", len(raw))
			continue
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
func (c *Client) record(s model.Stroke) {
	for {
		prev := c.LastSeen(s.Src)
		if s.StrokeID <= prev {
			return
		}
		if _, loaded := c.lastSeen.LoadOrStore(s.Src, s.StrokeID); loaded {
			// Replace only if still behind, so concurrent batches cannot
			// rewind the cursor.
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

package feed

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/heers-it/lightningfeed/internal/geo"
	"github.com/heers-it/lightningfeed/internal/model"
)

// SubjectPrefix is the root of this project's subject space. Versioning it in
// the subject lets a future incompatible payload be introduced alongside the old
// one rather than replacing it.
const SubjectPrefix = "lightning.v1"

// CloudEventType identifies a stroke event in CloudEvent structured mode.
//
// NATS has no native metadata envelope, so the payload carries one. CloudEvents
// is the portable convention for exactly this shape of bridge, and it keeps the
// idempotency key in a standard field instead of a bespoke one.
const CloudEventType = "org.blitzortung.lightning.stroke.v1"

// Attribution is reproduced from the upstream's own copyright string, which
// states that the data is Blitzortung.org contributors' work under CC BY-SA 4.0
// and may not be republished elsewhere.
//
// It travels in every message rather than only in a README because an
// attribution nobody receives does not identify anything.
const Attribution = "Lightning data (c) Blitzortung.org contributors, CC BY-SA 4.0"

// CellPrecision is the geohash precision used for subject routing.
//
// Precision 5 was chosen by measurement: a 10 km radius needs 30 cells and
// delivers about 1.87 times the circle's worth of strokes, which an exact radius
// test then trims. Precision 4 needs only two subjects but passes four times as
// much; precision 6 would need 693 subjects.
const CellPrecision = 5

// Options configures a Publisher.
type Options struct {
	// NATS is the connection to publish on. Required.
	NATS *nats.Conn

	// Stream is the JetStream stream to ensure. Created if absent.
	Stream string

	// SubjectPrefix overrides the subject root. Defaults to SubjectPrefix.
	SubjectPrefix string

	// Cells restricts which geohash cells are published. Empty publishes
	// everything, which is the world-wide deployment.
	//
	// A region is expressed here rather than by filtering the subject, so that
	// subscribers only ever see the cells they asked for.
	Cells []string

	// Region, when set, adds distance and certainty to each event.
	Region geo.Circle

	// BoundaryPolicy decides whether uncertain strokes count as inside.
	BoundaryPolicy geo.BoundaryPolicy
}

func (o Options) withDefaults() Options {
	if o.SubjectPrefix == "" {
		o.SubjectPrefix = SubjectPrefix
	}
	if o.Stream == "" {
		o.Stream = "LIGHTNING"
	}
	if o.BoundaryPolicy == "" {
		o.BoundaryPolicy = geo.PolicyInclude
	}
	return o
}

// Publisher publishes strokes as CloudEvents on NATS.
type Publisher struct {
	opts  Options
	js    nats.JetStreamContext
	cells map[string]struct{}
}

// NewPublisher ensures the stream exists and returns a Publisher.
func NewPublisher(opts Options) (*Publisher, error) {
	opts = opts.withDefaults()
	if opts.NATS == nil {
		return nil, fmt.Errorf("feed: a NATS connection is required")
	}

	js, err := opts.NATS.JetStream()
	if err != nil {
		return nil, fmt.Errorf("feed: jetstream is unavailable: %w", err)
	}

	p := &Publisher{opts: opts, js: js, cells: geo.ToSet(opts.Cells)}

	if err := p.ensureStream(); err != nil {
		return nil, err
	}
	return p, nil
}

// ensureStream creates the stream if it does not already exist.
//
// The duplicate window is the third of four deduplication layers, and it is the
// only one that survives a process restart: publishing the same stroke twice
// with the same message id is dropped by the broker rather than by any memory
// this process happens to still hold.
func (p *Publisher) ensureStream() error {
	info, err := p.js.StreamInfo(p.opts.Stream)
	if err == nil {
		// Adopt an existing stream, but refuse to run if its duplicate window is
		// too short to absorb a reconnect replay.
		if info.Config.Duplicates < duplicateWindow {
			return fmt.Errorf(
				"feed: stream %q has duplicate_window %v, need at least %v; "+
					"a short window would let a reconnect replay duplicate strokes",
				p.opts.Stream, info.Config.Duplicates, duplicateWindow)
		}
		return nil
	}

	_, err = p.js.AddStream(&nats.StreamConfig{
		Name:        p.opts.Stream,
		Subjects:    []string{p.opts.SubjectPrefix + ".>"},
		Storage:     nats.FileStorage,
		Retention:   nats.LimitsPolicy,
		MaxAge:      maxAge,
		MaxMsgs:     maxMsgs,
		Duplicates:  duplicateWindow,
		Discard:     nats.DiscardNew,
		AllowDirect: true,
		MaxMsgSize:  maxMsgSize,
	})
	if err != nil {
		return fmt.Errorf("feed: creating stream %q: %w", p.opts.Stream, err)
	}
	return nil
}

const (
	// duplicateWindow covers the upstream's reconnect replay. The window has to
	// exceed the five minutes the upstream replays, plus the reconnect backoff.
	duplicateWindow = 2 * time.Hour

	// maxAge matches the upstream's own display window and is ample for
	// "what was happening during my outage". Longer history lives in SQLite.
	maxAge = 2 * time.Hour

	// maxMsgs is a backstop against a runaway, not a working estimate; at the
	// observed rates the stream will not approach it.
	maxMsgs = 5_000_000

	// maxMsgSize comfortably fits the largest observed batch.
	maxMsgSize = 4 * 1024 * 1024
)

// StrokeData is the CloudEvent data payload.
type StrokeData struct {
	ID      int64   `json:"id"`
	Src     int     `json:"src"`
	Network string  `json:"network"`
	Time    string  `json:"time"`
	Lat     float64 `json:"lat"`
	Lon     float64 `json:"lon"`

	// DeviationM is the upstream's location uncertainty estimate. It is
	// omitted when the upstream did not report one, rather than sent as zero:
	// an absent estimate and a confident zero are different facts, and treating
	// them alike would make the feed look surer than it is.
	DeviationM *int `json:"deviation_m,omitempty"`

	DelayMS        *int `json:"delay_ms,omitempty"`
	ServingBackend *int `json:"serving_backend,omitempty"`

	// DistanceKm and Certainty are present only when a region is configured.
	DistanceKm *float64      `json:"distance_km,omitempty"`
	Certainty  geo.Certainty `json:"certainty,omitempty"`

	// ReceivedAt is when this bridge published the stroke. Time is when the
	// discharge happened; keeping both lets a consumer measure staleness
	// without guessing.
	ReceivedAt string `json:"received_at"`

	Attribution string `json:"attribution"`
}

// Event is the CloudEvent envelope.
type Event struct {
	SpecVersion     string     `json:"specversion"`
	Type            string     `json:"type"`
	Source          string     `json:"source"`
	ID              string     `json:"id"`
	Subject         string     `json:"subject"`
	Time            string     `json:"time"`
	DataContentType string     `json:"datacontenttype"`
	Data            StrokeData `json:"data"`
}

// Publish sends one stroke.
//
// The subject is derived from the stroke's own coordinates rather than from any
// precomputed cell, so a consumer that recomputes the subject must get the same
// answer.
func (p *Publisher) Publish(ctx context.Context, s model.Stroke) error {
	subject, err := p.SubjectFor(s)
	if err != nil {
		return err
	}

	event := Event{
		SpecVersion:     "1.0",
		Type:            CloudEventType,
		Source:          p.opts.SubjectPrefix,
		ID:              s.Key(),
		Subject:         subject,
		Time:            s.Time.UTC().Format(time.RFC3339Nano),
		DataContentType: "application/json",
		Data:            p.dataFor(s),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("feed: encoding event: %w", err)
	}

	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		// The message id is the upstream's own identity, so the broker's
		// duplicate window absorbs a reconnect replay with no bookkeeping of our
		// own.
		Header: nats.Header{nats.MsgIdHdr: []string{event.ID}},
	}
	if _, err := p.js.PublishMsg(msg); err != nil {
		return fmt.Errorf("feed: publishing %s: %w", event.ID, err)
	}
	return nil
}

// dataFor builds the event payload for a stroke.
func (p *Publisher) dataFor(s model.Stroke) StrokeData {
	d := StrokeData{
		ID:             s.StrokeID,
		Src:            int(s.Src),
		Network:        s.Network(),
		Time:           s.Time.UTC().Format(time.RFC3339Nano),
		Lat:            s.Lat,
		Lon:            s.Lon,
		DeviationM:     s.DeviationM,
		DelayMS:        s.DelayMS,
		ServingBackend: s.ServingBackend,
		ReceivedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		Attribution:    Attribution,
	}

	if p.opts.Region.RadiusKm > 0 {
		dist, certainty := p.opts.Region.ClassifyPoint(s.Lat, s.Lon, devValue(s.DeviationM))
		d.DistanceKm = &dist
		d.Certainty = certainty
	}
	return d
}

func devValue(p *int) float64 {
	if p == nil {
		return 0
	}
	return float64(*p)
}

// SubjectFor returns the subject a stroke is published to.
func (p *Publisher) SubjectFor(s model.Stroke) (string, error) {
	cell, err := geo.Encode(s.Lat, s.Lon, CellPrecision)
	if err != nil {
		return "", fmt.Errorf("feed: encoding cell for stroke %s: %w", s.Key(), err)
	}
	if !geo.InCells(p.cells, cell) {
		return "", fmt.Errorf("feed: stroke %s at %v,%v is in cell %q, outside the configured region",
			s.Key(), s.Lat, s.Lon, cell)
	}
	return p.opts.SubjectPrefix + ".src." + strconv.Itoa(int(s.Src)) + ".cell." + cell, nil
}

// PublishBatch sends a slice of strokes, returning the first error.
func (p *Publisher) PublishBatch(ctx context.Context, strokes []model.Stroke) error {
	for _, s := range strokes {
		if err := p.Publish(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// Sync waits for the server to acknowledge everything published so far.
func (p *Publisher) Sync(ctx context.Context) error {
	if err := p.opts.NATS.FlushWithContext(ctx); err != nil {
		return fmt.Errorf("feed: flushing: %w", err)
	}
	return nil
}

// Cells returns the geohash cells this publisher is restricted to, if any.
func (p *Publisher) Cells() []string {
	out := make([]string, 0, len(p.cells))
	for c := range p.cells {
		out = append(out, c)
	}
	return out
}

// Package upstream speaks the undocumented lightningmaps.org real-time feed.
//
// The feed is a plain JSON WebSocket at wss://live.lightningmaps.org:443/ with
// no authentication. What follows is the protocol as observed on 2026-10-04.
package upstream

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/heers-it/lightningfeed/internal/model"
)

// ProtocolVersion is the subscribe-frame version this client speaks.
//
// It is sent verbatim and the upstream will drop the connection if the frame is
// malformed, so it is pinned rather than discovered. If the upstream changes
// protocol this constant is the thing to bump.
const ProtocolVersion = 24

// SrcMask selects which networks to subscribe to.
//
// A mask bit is NOT the same number as the source code its strokes carry. The
// bit position and the stroke's `src` field are separate identifiers that merely
// happen to run in the same order, so the mapping has to be spelled out:
//
//	bit 0 (mask 1)  reserved   strokes with src:0, none ever observed
//	bit 1 (mask 2)  Blitzortung.org   strokes with src:1
//	bit 2 (mask 4)  LightningMaps.org strokes with src:2
//	bit 3 (mask 8)  testing           strokes with src:8, none observed
//
// Confusing the two produces a client that subscribes to one network and
// receives another, with no error anywhere.
type SrcMask int

const (
	// MaskReserved is accepted by the upstream but carried no data when
	// observed.
	MaskReserved SrcMask = 1 << 0

	// MaskBlitzortung selects the Blitzortung.org network, whose strokes carry
	// src:1.
	MaskBlitzortung SrcMask = 1 << 1

	// MaskLightningMaps selects the LightningMaps.org network, whose strokes
	// carry src:2. This is the upstream's own default and what this project
	// uses.
	MaskLightningMaps SrcMask = 1 << 2

	// MaskTesting selects the upstream's test network. The WebSocket accepts
	// it and returns nothing; the HTTP fallback rejects the whole request.
	MaskTesting SrcMask = 1 << 3
)

// SrcForMask returns the source code the upstream reports for strokes selected by
// a single mask bit, and whether that bit selects anything at all.
func (m SrcMask) SrcForMask() (model.Source, bool) {
	switch m {
	case MaskBlitzortung:
		return model.SourceBlitzortung, true
	case MaskLightningMaps:
		return model.SourceLightningMaps, true
	case MaskReserved, MaskTesting:
		// Both are accepted but carried no data when observed.
		return model.Source(0), false
	default:
		return model.Source(0), false
	}
}

// DefaultSrcMask is the source selection this project subscribes with.
const DefaultSrcMask = MaskLightningMaps

// FrameKind distinguishes the three shapes the upstream sends.
type FrameKind int

const (
	// FrameStrokes is a batch of located strokes.
	FrameStrokes FrameKind = iota

	// FrameHeartbeat is a bare time frame sent roughly every ten seconds to
	// keep the connection warm. It carries no strokes.
	FrameHeartbeat

	// FrameHello is the first frame on a connection, carrying the session id,
	// the current client count and the backend port.
	FrameHello
)

// String makes frame kinds readable in logs.
func (k FrameKind) String() string {
	switch k {
	case FrameStrokes:
		return "strokes"
	case FrameHeartbeat:
		return "heartbeat"
	case FrameHello:
		return "hello"
	default:
		return "unknown"
	}
}

// Hello is the upstream's opening frame.
type Hello struct {
	// ClientID is the session id assigned by the upstream.
	ClientID int `json:"cid"`

	// Connected is the number of clients currently on this backend.
	Connected int `json:"con"`

	// BackendPort is the internal port the connection landed on.
	BackendPort string `json:"port"`

	// ServerTime is the upstream's clock, in Unix seconds.
	ServerTime float64 `json:"time"`

	// Keepalive is a session token. This client does not answer it: the
	// upstream streams identically whether the token is replied to correctly,
	// replied to with garbage, or ignored, so there is no reason to reproduce
	// the transform. It is captured for diagnostics only.
	Keepalive float64 `json:"k"`
}

// Frame is one decoded upstream message.
type Frame struct {
	Kind FrameKind

	// ServerTime is the upstream's clock in Unix seconds, when the frame
	// carries one. It is useful for measuring feed latency.
	ServerTime float64

	// Flags is an opaque per-source bitmask. Observed values are 0 and 2, and
	// the upstream uses it to tell clients when to re-derive their delay
	// estimate. Nothing downstream needs it, so it is carried but not
	// interpreted.
	Flags map[string]int

	Strokes []model.Stroke

	Hello Hello
}

// rawFrame mirrors the upstream JSON. Fields absent from a frame are simply
// absent here, which is how heartbeats and the hello are told apart from stroke
// batches.
type rawFrame struct {
	Cid  int     `json:"cid"`
	Con  int     `json:"con"`
	Port string  `json:"port"`
	K    float64 `json:"k"`

	Time    float64        `json:"time"`
	Flags   map[string]int `json:"flags"`
	Strokes []rawStroke    `json:"strokes"`
}

type rawStroke struct {
	// Time is Unix milliseconds on the WebSocket, and relative milliseconds on
	// the HTTP endpoint. ParseFrame only handles the WebSocket form; the HTTP
	// transport normalises relative times before they reach here.
	Time int64 `json:"time"`

	// The WebSocket sends numbers and the HTTP endpoint sends strings, so both
	// are accepted rather than silently coercing one to zero.
	Lat flexFloat `json:"lat"`
	Lon flexFloat `json:"lon"`

	Src int   `json:"src"`
	ID  int64 `json:"id"`

	Dev *int `json:"dev"`
	Del *int `json:"del"`
	Srv *int `json:"srv"`
	Alt *int `json:"alt"`
}

// flexFloat decodes a JSON number or a JSON string containing a number.
//
// The upstream is inconsistent about this between its two transports, and a
// plain float64 would quietly yield 0.0 for the string form, placing every
// stroke at the null island and counting it as outside the region.
type flexFloat float64

func (f *flexFloat) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("upstream: empty numeric value")
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if s == "" {
			*f = 0
			return nil
		}
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("upstream: %q is not a number", s)
		}
		*f = flexFloat(v)
		return nil
	}

	var v float64
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*f = flexFloat(v)
	return nil
}

// ParseFrame decodes one upstream WebSocket frame.
//
// A malformed frame returns an error but must not be treated as fatal by the
// caller: one bad frame is a hiccup, not an outage.
func ParseFrame(raw []byte) (Frame, error) {
	var rf rawFrame
	if err := json.Unmarshal(raw, &rf); err != nil {
		return Frame{}, fmt.Errorf("upstream: decoding frame: %w", err)
	}

	frame := Frame{
		ServerTime: rf.Time,
		Flags:      rf.Flags,
	}

	switch {
	case rf.Strokes != nil:
		frame.Kind = FrameStrokes
		frame.Strokes = make([]model.Stroke, 0, len(rf.Strokes))
		for i, rs := range rf.Strokes {
			s, err := rs.normalise()
			if err != nil {
				return Frame{}, fmt.Errorf("upstream: stroke %d: %w", i, err)
			}
			frame.Strokes = append(frame.Strokes, s)
		}
	case rf.K != 0 || rf.Cid != 0:
		frame.Kind = FrameHello
		frame.Hello = Hello{
			ClientID:    rf.Cid,
			Connected:   rf.Con,
			BackendPort: rf.Port,
			ServerTime:  rf.Time,
			Keepalive:   rf.K,
		}
	default:
		// A bare time frame: the upstream's keepalive.
		frame.Kind = FrameHeartbeat
	}

	return frame, nil
}

// normalise converts one wire stroke into the internal representation.
func (rs rawStroke) normalise() (model.Stroke, error) {
	lat := float64(rs.Lat)
	lon := float64(rs.Lon)

	if math.IsNaN(lat) || math.IsNaN(lon) {
		return model.Stroke{}, fmt.Errorf("coordinate is not a number: lat=%v lon=%v", lat, lon)
	}
	if lat < -90 || lat > 90 {
		return model.Stroke{}, fmt.Errorf("latitude %v is outside [-90, 90]", lat)
	}
	if lon < -180 || lon > 180 {
		return model.Stroke{}, fmt.Errorf("longitude %v is outside [-180, 180]", lon)
	}
	if rs.Time <= 0 {
		return model.Stroke{}, fmt.Errorf("stroke time %d is not a valid Unix millisecond timestamp", rs.Time)
	}

	return model.Stroke{
		Src:            model.Source(rs.Src),
		StrokeID:       rs.ID,
		Time:           time.UnixMilli(rs.Time).UTC(),
		Lat:            lat,
		Lon:            lon,
		DeviationM:     rs.Dev,
		DelayMS:        rs.Del,
		ServingBackend: rs.Srv,
	}, nil
}

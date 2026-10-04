# lightningfeed

Bridges the [lightningmaps.org](https://www.lightningmaps.org) real-time lightning
feed into a NATS JetStream stream and a SQLite archive.

Built for a single region by default: every stroke within **10 km of Roussospiti,
Crete** (35.3340688, 24.4944483).

---

## Attribution and terms

**Lightning data © Blitzortung.org contributors, licensed [CC BY-SA
4.0](https://creativecommons.org/licenses/by-sa/4.0/).**

This project is a **private, non-commercial** deployment. Please read the
upstream's terms before reusing it:

- Non-commercial, private and entertainment use only.
- **Not** for storm warning systems, overvoltage plausibility checks, or risk
  analysis for protecting technology. These are explicitly prohibited even when
  the data come via a third party.
- The upstream's terms require that consumers **read from a separate server
  rather than connecting to Blitzortung directly**. That is exactly what this
  bridge is: it holds the single upstream connection and republishes internally.
- Attribution must travel with the data. It is included in every published
  message as `data.attribution`.

Before making this public, or using it commercially, obtain written permission.
The upstream asks that such requests go through
[their forum](https://forum.blitzortung.org/) rather than email.

The upstream feed is **undocumented and unauthenticated**. It can change or be
gated at any time. If that happens, the pipeline fails loudly rather than
silently — see [Observability](#observability).

## What it does

```
wss://live.lightningmaps.org  ──▶  backfill drop ──▶ (src,id) dedup
   one connection, leader only                            │
                                                          ▼
                            geohash-5 cells ──▶ region filter ──▶ certainty band
                                                          │
                                    ┌─────────────────────┴──────────────┐
                                    ▼                                    ▼
                      NATS JetStream (2 h, dedup 2 h)          SQLite (7 days)
                      CloudEvents, one per stroke               radius query
```

Four layers suppress replayed strokes, because the upstream **replays about five
minutes of history on every connection**:

1. **Backfill drop** — discard the replay outright at connect time
2. **Identity filter** — remember `(source, id)` and reject a repeat
3. **JetStream duplicate window** — the broker drops a republished id; survives restarts
4. **SQLite `UNIQUE (src, stroke_id)`** — durable, and the last line of defence

## Install

```bash
go build -o lightningfeed ./cmd/lightningfeed
```

Go 1.25 or newer. The binary is static: SQLite is the pure-Go `modernc.org/sqlite`
driver, so there is no cgo and no C toolchain on the target.

## Run

```bash
lightningfeed ingest \
  --nats-url=nats://127.0.0.1:4222 \
  --region-name=roussospiti \
  --region-lat=35.3340688 \
  --region-lon=24.4944483 \
  --region-radius-km=10 \
  --sqlite=/var/lib/lightningfeed/lightningfeed.db
```

Every flag has a `LIGHTNINGFEED_*` environment equivalent, and the environment
wins, so secrets need not appear in a unit file:

| Flag | Environment |
|---|---|
| `--nats-url` | `LIGHTNINGFEED_NATS_URL` |
| `--stream` | `LIGHTNINGFEED_STREAM` |
| `--region-lat` / `--region-lon` / `--region-radius-km` | `LIGHTNINGFEED_REGION_LAT` … |
| `--boundary-policy` | `LIGHTNINGFEED_BOUNDARY_POLICY` |
| `--sqlite` | `LIGHTNINGFEED_SQLITE` |
| `--src-mask` | `LIGHTNINGFEED_SRC_MASK` |
| `--publish-attempts` / `--publish-backoff` | `LIGHTNINGFEED_PUBLISH_ATTEMPTS` … |
| `--log-level` / `--log-format` | `LIGHTNINGFEED_LOG_LEVEL` … |

Omit all three `--region-*` flags for a world-wide feed. Supplying only some of
them is an error rather than a silent world-wide feed, so a typo cannot quietly
turn a regional deployment into a global one.

A region on the equator or the prime meridian is legitimate: a `0` is treated as
supplied, not missing.

**The radius is capped at 2500 km, and the cell enumeration at 2 million cells.**
Cell enumeration is O(area) and its result is held in memory, written to the
archive as JSON, and re-parsed on every radius query. The radius cap alone is not
enough, because a degree of longitude covers fewer kilometres the further you get
from the equator — the *same* 2500 km radius is 1.1 M cells at the equator and
8.0 M (1.5 GiB) at latitude 60. So the second bound is on the cell count, which is
the thing that actually costs. If you want more than either allows, omit the region
and publish world-wide, which needs no cell list at all.

Regions that cross the antimeridian (Fiji, Kiribati, the Chatham Islands) are
handled: the cell list wraps.

**JetStream is not optional.** The publisher registers a message id per stroke and
relies on the broker's duplicate window to absorb a reconnect replay, which Core
NATS cannot do; startup fails outright if JetStream is unavailable. There is
deliberately no `--jetstream` flag, because it could only ever select a failure.

**A failed publish is retried, then fatal.** `--publish-attempts` (default 3)
tries with `--publish-backoff` (default 250 ms) between attempts, so a brief NATS
blip is invisible and a sustained outage still stops the pipeline. Retrying
forever would turn an outage into a silent, unbounded backlog; not retrying at
all would drop strokes over a hiccup. Exhausting the attempts is fatal on
purpose.

### Other commands

```bash
# the subjects a region subscriber needs
lightningfeed cells --region-name=roussospiti \
  --region-lat=35.3340688 --region-lon=24.4944483 --region-radius-km=10

# history near the region, straight from SQLite
lightningfeed history --sqlite=/var/lib/lightningfeed/lightningfeed.db \
  --region-lat=35.3340688 --region-lon=24.4944483 --region-radius-km=10 \
  --since=24h
```

`cells` prints **every source the `--src-mask` selects**, because the subject
carries the source code and the two networks issue independent id sequences:

```
$ lightningfeed cells --region-name=munich --region-lat=48 --region-lon=11 \
    --region-radius-km=1 --src-mask=6
munich: 1 cells, radius 1 km around 48.0000,11.0000
sources: src.1, src.2

blitzortung.org
lightning.v1.src.1.cell.u0xc4

lightningmaps.org
lightning.v1.src.2.cell.u0xc4
```

The reserved and testing mask bits carry no data and are skipped, since printing
them would list subjects nothing is ever published to.

`history` reads SQLite rather than NATS because a **radius query is not something
subject filtering can express**.

`--limit` bounds how many strokes are printed, keeping the **most recent** ones;
omit it for everything in the window. A negative value is rejected rather than
treated as unlimited.

### There is no upstream failover

There is deliberately no failover URL, and `live2` is never dialled
automatically. The two servers issue **independent id sequences** — the same
minute showed a last id near 1.48M on one and near 17.8M on the other — so a
cursor carried between them is not a resume but a claim about an unrelated
sequence, and the `(src, id)` identity filter cannot help because colliding ids
from different servers name different strokes.

The stored cursor therefore records which server issued it, and on startup:

| Stored cursor | Behaviour |
|---|---|
| from the configured upstream | resumed; no five-minute replay |
| from a **different** upstream | refused, with a warning; starts cold |

Starting cold is safe rather than fatal: the backfill window drops the replay, so
the cost of a repointed `--upstream-url` is one window of backfill, not a
pipeline that will not start. Republishing history as live data — the one failure
this whole pipeline exists to prevent — cannot happen either way. If you want the
second server, set `--upstream-url` to it deliberately and accept the cold start.

Losing the lease to another process **stops the pipeline and exits non-zero**. It
has to: the process can only stop itself by cancelling, and a cancelled pipeline
looks identical to a deliberate shutdown. Exiting 0 would mean a supervisor's
`Restart=on-failure` brought nothing back, and the bridge would have quietly
stopped feeding data while every exit signal said otherwise.

## Consuming

Messages are CloudEvents, one per stroke, on
`lightning.v1.src.<n>.cell.<geohash5>`.

**Read through a JetStream consumer, not a plain Core NATS subscription.**
Deduplication applies to the stream, not the subject: a Core subscription sees
every publish including replays. This is asserted in the test suite so it cannot
regress quietly.

```go
js, _ := nc.JetStream()
sub, _ := js.Subscribe("LIGHTNING", nats.Durable("my-consumer"))
for msg := range sub.Messages() {
    var ev feed.Event
    _ = json.Unmarshal(msg.Data, &ev)
    fmt.Println(ev.Data.Lat, ev.Data.Lon, ev.Data.Certainty)
}
```

## The certainty band

Each stroke carries the upstream's own location-uncertainty estimate (`dev`,
median ≈ 2.2 km, observed up to 15 km). At a 10 km radius that makes membership
genuinely **undecidable** for some strokes: one reported 9.9 km out with 2 km of
uncertainty may physically be inside, and one reported 0.5 km out with 15 km of
uncertainty may be far outside.

So every stroke is classified rather than forced into a boolean:

| Band | Rule | Meaning |
|---|---|---|
| `in` | `dist + dev ≤ radius` | certainly inside |
| `boundary` | `dist - dev ≤ radius < dist + dev` | undecidable |
| `out` | `dist - dev > radius` | certainly outside |

`dev` is bounded: a negative figure counts as zero and one above 100 km is capped.
`dev` arrives as an optional integer, so `INT_MAX` — the commonest "no data"
sentinel in this protocol family — is representable, and unclamped it would make
every stroke within `radius + dev` undecidable, which the default policy accepts.
One stroke carrying a sentinel would then publish the whole planet.

**`boundary` strokes count as inside** (`--boundary-policy include`, the
default): a missed stroke is worse than a slightly misplaced one. The band is
still carried on every message and stored per row, so `--boundary-policy exclude`
switches to strict in-circle-only with no rebuild, and
`lightningfeed_dropped_total` tells you what it would cost.

## Operational constraints

These are measured against the live upstream, not assumed. The configuration
refuses values that would break them.

**The upstream throttles new connections.**

| Gap between attempts | Successes |
|---|---|
| 0 ms | **0 / 6** |
| 5 s | 2 / 6 |
| 15 s | **6 / 6** |

Failures are silent hangs, not errors. Hence `--reconnect-min` has a **15 s
floor**, enforced at startup, and **exactly one process connects** — the
leadership lease guarantees it. Two ingest instances would spend their lives
fighting over a connection neither could hold.

**The upstream keeps connections open with a bare `{"time":…}` frame roughly
every ten seconds**, so a healthy connection is never idle. `--idle-timeout`
(45 s default) is the watchdog for the case where a connection goes silent
without closing.

**The upstream's geo-filter is loose.** A 1.3° × 1.5° request was observed
returning strokes spanning 7.7° × 7.0°, with only ~14% of the live stream landing
inside the requested box. `--bbox` is sent to reduce load; exact filtering is
always local.

**Its two transports disagree on types** — the WebSocket sends coordinates as
JSON numbers, the HTTP long-poll fallback as strings. Both are accepted; a plain
`float64` would have silently yielded `0.0` and placed every fallback stroke at
the null island.

**A coordinate that is not a coordinate is rejected, not coerced.** `null`, `""` and
an absent field are all dropped per stroke and counted as `bad_coordinate`; a
genuine `0` is a real coordinate and is kept. The absent case is the one that
matters: the feed is undocumented and can change at any time, and a renamed field
would otherwise arrive as a stream of lightning reported off the coast of Africa,
with no error anywhere.

## Observability

- `GET :9109/metrics` — Prometheus
- `GET :9109/healthz` — readiness. A standby reports **not ready** on purpose: it
  is working correctly and should not be sent traffic.

Readiness also goes false when the **store stops answering**, and recovers on the
next successful archive write. A failed archive write is otherwise deliberately
tolerated — it costs a missed query rather than a lost stroke — so without this the
probe would report a store that had been failing for an hour as healthy.

| Metric | Watches |
|---|---|
| `lightningfeed_upstream_connected` | 1 while an upstream connection is live, 0 while reconnecting |
| `lightningfeed_upstream_last_message_age_seconds` | distinguishes quiet from dead |
| `lightningfeed_reconnects_total{host,reason}` | reconnection attempts |
| `lightningfeed_malformed_frames_total` | frames that failed to decode entirely |
| `lightningfeed_rejected_strokes_total` | strokes dropped from an otherwise decodable frame |
| `lightningfeed_dropped_total{reason}` | `replay`, `outside_region`, `bad_coordinate`, `store_error` |
| `lightningfeed_leader` | whether this process is the one connecting |
| `lightningfeed_strokes_total`, `lightningfeed_published_total` | volume |
| `lightningfeed_publish_lag_ms` | stroke time to publication |
| `lightningfeed_store_rows` | archive depth |
| `lightningfeed_store_pruned_total` | retention pruning |
| `lightningfeed_config_info{region}` | the region actually in force |

`upstream_last_message_age_seconds` and `upstream_connected` are deliberately
separate. Age alone cannot distinguish a quiet region from a dead connection, and
the connection gauge cannot survive the moment a socket drops — merging them
would have made the one signal that matters during an outage read as healthy.

`malformed_frames_total` and `rejected_strokes_total` are separate for the same
reason, in the other direction: a frame that decodes but contains one
unusable coordinate is *not* a malformed frame. The decoder rejects the offending
stroke and keeps its siblings, so the two counters answer "was the stream
unreadable" and "how much of a readable stream was unusable" independently.

Alerts worth having: `upstream_last_message_age > 60s` for 5 min;
`dropped/published > 5%`; no leader for 2 min.

## Testing

```bash
go test ./...                       # unit and integration, no network
go test -race ./...
go test -tags e2e -timeout 5m ./internal/e2e/ -run TestLive -v   # against the live upstream

# fuzzing. FuzzCircleCells needs -parallel: each worker allocates up to the cell
# budget while probing it, so one worker per core exhausts memory.
go test -run FuzzParseFrame -fuzz FuzzParseFrame -fuzztime=90s ./internal/upstream/
go test -run FuzzStrokeCoordinate -fuzz FuzzStrokeCoordinate -fuzztime=90s ./internal/upstream/
go test -run FuzzCircleCells -fuzz FuzzCircleCells -fuzztime=120s -parallel=4 ./internal/geo/
```

The `e2e` tests are behind a build tag and excluded from `go test ./...`
deliberately: the upstream's connection tolerance is finite, and a suite opening
several connections in sequence consumes everyone else's share. **Do not add
live-feed tests to CI.**

Tests run against real infrastructure where it matters: an embedded
`nats-server` with file-backed JetStream, a real SQLite file in a temp directory,
and a real WebSocket server replaying frames captured from the live feed. The
protocol decoder's golden fixtures were captured on 2026-10-04, so protocol
drift fails a test instead of surfacing quietly at 3 a.m.

## Layout

```
cmd/lightningfeed/      CLI: ingest, history, cells
internal/
  geo/                  geohash, haversine, circle cells, certainty bands
  upstream/             protocol decoder and reconnecting WebSocket client
  dedup/                backfill window and (src,id) suppression
  feed/                 CloudEvent publisher, JetStream stream
  store/                SQLite cursor and spatial archive
  elect/                leadership lease
  obs/                  metrics and health
  ingest/               wiring: the only package that knows the order
  e2e/                  live-upstream tests, behind the e2e tag
  testsupport/          embedded NATS, fake WebSocket, golden fixtures
```
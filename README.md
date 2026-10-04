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
| `--region-lat` / `--region-lon` / `--region-radius-km` | `LIGHTNINGFEED_REGION_LAT` … |
| `--boundary-policy` | `LIGHTNINGFEED_BOUNDARY_POLICY` |
| `--sqlite` | `LIGHTNINGFEED_SQLITE` |
| `--src-mask` | `LIGHTNINGFEED_SRC_MASK` |
| `--log-level` / `--log-format` | `LIGHTNINGFEED_LOG_LEVEL` … |

Omit all three `--region-*` flags for a world-wide feed.

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

`history` reads SQLite rather than NATS because a **radius query is not something
subject filtering can express**.

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

## Observability

- `GET :9109/metrics` — Prometheus
- `GET :9109/healthz` — readiness. A standby reports **not ready** on purpose: it
  is working correctly and should not be sent traffic.

| Metric | Watches |
|---|---|
| `lightningfeed_upstream_last_message_age_seconds` | distinguishes quiet from dead |
| `lightningfeed_dropped_total{reason}` | `replay`, `outside_region`, `bad_coordinate`, `store_error` |
| `lightningfeed_leader` | whether this process is the one connecting |
| `lightningfeed_strokes_total`, `lightningfeed_published_total` | volume |
| `lightningfeed_store_rows` | archive depth |

Alerts worth having: `upstream_last_message_age > 60s` for 5 min;
`dropped/published > 5%`; no leader for 2 min.

## Testing

```bash
go test ./...                       # unit and integration, no network
go test -race ./...
go test -tags e2e -timeout 5m ./internal/e2e/ -run TestLive -v   # against the live upstream
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
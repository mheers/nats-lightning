# nats-lightning

Real-time lightning strokes onto NATS JetStream, from the lightningmaps.org feed.

One connection to the upstream, republished as CloudEvents on a subject space
narrowed to a region of your choosing, archived in SQLite for radius queries. Ships
with a demo consumer, a Dockerfile and a compose file.

The binaries are called `lightningfeed` and `lightningfeed-demo`; the repository is
`nats-lightning`.

---

## Attribution and terms — read this first

**The code is MIT licensed. The data is not, and the data is the part with
restrictions.**

**Lightning data © Blitzortung.org contributors, licensed
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/), and redistributed
under the terms published by [lightningmaps.org](https://lightningmaps.org).**

The upstream's terms permit private, non-commercial, entertainment use only, and
explicitly prohibit:

- storm warning systems
- overvoltage plausibility checks
- risk analysis for protecting technology

Those prohibitions apply to whatever you build on top of this, whatever the
[LICENSE](LICENSE) says about the code. **This is not a storm warning system**,
and it could not responsibly be one: each stroke carries kilometres of location
uncertainty, and the bridge reports that uncertainty rather than hiding it — see
[The certainty band](#the-certainty-band).

The upstream's terms also require that consumers **read from a separate server
rather than connecting to Blitzortung directly**. That is what this bridge is: it
holds the single upstream connection and republishes internally. Attribution travels
with the data as `data.attribution` on every message, and the demo prints it.

The feed is **undocumented and unauthenticated**. It can change or be gated at any
time; if it is, this fails loudly rather than silently.

Anything commercial, or any use beyond the above, needs written permission first —
the upstream asks that such requests go through
[their forum](https://forum.blitzortung.org/) rather than email. This project is
offered as-is with no warranty and no claim of having been endorsed by anyone
upstream.

---

## Quick start

One command, on a fresh clone:

```bash
make demo
```

That creates `.env`, starts a broker and the bridge, waits for a live upstream
connection, and draws what arrives. It runs **world-wide** by default, because
world-wide is reliably busy — measured at roughly 20 strokes a second — while any
particular circle is quiet most of the time. A demo pointed at a quiet region shows
nothing at all, which looks exactly like a broken install.

To narrow it, put your coordinates in `.env` (the region block in `.env.example`
explains what the three values mean):

```bash
make demo NAME=munich LAT=48.14 LON=11.58 RADIUS_KM=25 DURATION=120s
```

That writes the region into `.env` and restarts the bridge, so both processes always
agree on which region is in force. It will show nothing unless there is a storm over
that circle right now, which is normal.

The demo draws a live map of the region in your terminal:

```
watching munich  48.1400,11.5800  within 25 km
stream "LIGHTNING" at nats://nats:4222
187 subject(s), consumer "lightningfeed-demo", from now
map 44x22, strokes fade after 1m30s

Lightning data (c) Blitzortung.org contributors, CC BY-SA 4.0

2m elapsed · 41 strokes · 20.5/min · last live · closest 0.83 km
in 38  ·  boundary 3

  
                    ········
               ··················
            ··············:··+······
          ···:························
         ······························
        ········+······+··+·············
       ········:·+*·+··+·+···············
      ··:········+·+···*··················
      ·········*··+·*:·+++················
      ·········**··:····@·+······:········
      ·········+·:······::················
       ·····*:····:······················
        ·+······························
         ···························:··
          ····················:·······
            ························
               ··················
                    ········
  
  * now   + recent   : fading   · inside radius   @ region centre

  ▁▂▁▂▃▅▇▆▄▂▅█▆▄▃▂▄▅▃▂▂▃▄▃▂▂▂▂▂▁  strokes per 10s, most recent on the right
```

It also works without a terminal, one line per stroke, which is what to use when
piping it:

```bash
docker compose --profile demo run --rm -T demo --view log
```

**A quiet region produces nothing for hours.** That is normal, not a fault, and the
demo says so rather than sitting silent.

### Make targets

`make` on its own lists them. The ones worth knowing:

| Target | What it does |
|---|---|
| `make demo` | the whole thing: broker, bridge, live demo |
| `make demo-log` | the same, as log lines for piping |
| `make up` / `make down` | start / stop the stack, deleting volumes on `down` |
| `make logs` / `make metrics` | follow the bridge's logs, scrape its metrics once |
| `make subjects` | print the subjects this region publishes to |
| `make build` | both binaries into `./bin` |
| `make test` / `make test-race` | the suite, with and without the race detector |
| `make lint` | `gofmt` check and `go vet` |
| `make ci` | everything CI runs |
| `make docker-build` | the container image for this machine |
| `make docker-push` | push to Docker Hub as `mheers/nats-lightning` |
| `make docker-pushx` | push a multi-architecture image |

`make demo` rebuilds the image before running, so it always shows the current source
rather than whatever was built last.

For `docker-push` the version is the last git tag, so a release is
`git tag && make docker-push` with nothing to edit. `latest` is a separate target,
because overwriting a mutable tag from a branch would leave it pointing at something
nobody tested. `docker login` first if this is your first push.

### Without Docker

```bash
go build -o lightningfeed ./cmd/lightningfeed
nats-server -js -sd /tmp/js

lightningfeed ingest \
  --nats-url=nats://127.0.0.1:4222 \
  --region-name=roussospiti \
  --region-lat=35.3340688 \
  --region-lon=24.4944483 \
  --region-radius-km=10 \
  --sqlite=/tmp/lightningfeed.db
```

Go 1.26 or newer. The binary is static: SQLite is the pure-Go `modernc.org/sqlite`
driver, so there is no cgo and no C toolchain on the target.

---

## Configuration

Every flag has a `LIGHTNINGFEED_*` environment equivalent, and **the environment
wins**, so a deployment can set values without writing them into a unit file's
arguments — and so `.env` can configure the bridge and the demo together without
either disagreeing about the region.

| Flag | Environment | Default |
|---|---|---|
| `--nats-url` | `LIGHTNINGFEED_NATS_URL` | `nats://127.0.0.1:4222` |
| `--stream` | `LIGHTNINGFEED_STREAM` | `LIGHTNING` |
| `--region-name` | `LIGHTNINGFEED_REGION_NAME` | the coordinates |
| `--region-lat` / `--region-lon` / `--region-radius-km` | `LIGHTNINGFEED_REGION_LAT` … | none, which is world-wide |
| `--boundary-policy` | `LIGHTNINGFEED_BOUNDARY_POLICY` | `include` |
| `--upstream-url` | `LIGHTNINGFEED_UPSTREAM_URL` | `wss://live.lightningmaps.org:443/` |
| `--src-mask` | `LIGHTNINGFEED_SRC_MASK` | `4` (lightningmaps.org) |
| `--sqlite` | `LIGHTNINGFEED_SQLITE` | `/var/lib/lightningfeed/lightningfeed.db` |
| `--retention` | `LIGHTNINGFEED_RETENTION` | `168h` |
| `--publish-attempts` / `--publish-backoff` | `LIGHTNINGFEED_PUBLISH_ATTEMPTS` … | `3` / `250ms` |
| `--leader-elect` | `LIGHTNINGFEED_LEADER_ELECT` | `true` |
| `--metrics-addr` | `LIGHTNINGFEED_METRICS_ADDR` | `:9109` |
| `--log-level` / `--log-format` | `LIGHTNINGFEED_LOG_LEVEL` … | `info` / `json` |
| `--reconnect-min` / `--reconnect-max` / `--idle-timeout` / `--handshake-timeout` | flags only | measured, see below |
| `--backfill-drop` / `--dedupe-ttl` / `--prune-every` / `--leader-key` | flags only | see `internal/config` |

`.env.example` documents each one, and ships with the region commented out: a
world-wide feed shows something the moment it starts, and a specific circle is quiet
most of the time. Every variable in it is honoured by the parser, and a variable that
is not recognised is a startup error rather than a silent default.

**A word where a flag was expected is an error**, not a silently discarded
argument. A dropped `--region-radius-km` that quietly left the default in place is
much harder to notice than a refusal.

### The region

Omit all three `--region-*` flags for a world-wide feed. Supplying only some of them
is an error rather than a silent world-wide feed, so a typo cannot quietly turn a
regional deployment into a global one. A region on the equator or the prime meridian
is legitimate: a `0` is treated as supplied, not missing.

**The radius is capped at 2500 km, and the cell enumeration at 2 million cells.**
Cell enumeration is O(area) and its result is held in memory, written to the archive
as JSON, and re-parsed on every radius query. The radius cap alone is not enough,
because a degree of longitude covers fewer kilometres the further you get from the
equator — the *same* 2500 km radius is 1.1 M cells at the equator and 8.0 M
(1.5 GiB) at latitude 60. So the second bound is on the cell count, which is the
thing that actually costs. If you want more than either allows, omit the region and
publish world-wide, which needs no cell list at all.

Regions that cross the antimeridian (Fiji, Kiribati, the Chatham Islands) are
handled: the cell list wraps.

A world-wide feed runs at roughly 20 strokes a second and the archive holds about
1.5 kB per stroke, so the default seven-day retention is several GB. Narrow the
region, or lower `LIGHTNINGFEED_RETENTION`, if that matters.

**JetStream is not optional.** The publisher registers a message id per stroke and
relies on the broker's duplicate window to absorb a reconnect replay, which Core NATS
cannot do; startup fails outright if JetStream is unavailable. There is deliberately
no `--jetstream` flag, because it could only ever select a failure.

**A failed publish is retried, then fatal.** `--publish-attempts` (default 3) tries
with `--publish-backoff` (default 250 ms) between attempts, so a brief NATS blip is
invisible and a sustained outage still stops the pipeline. Retrying forever would turn
an outage into a silent, unbounded backlog; not retrying at all would drop strokes
over a hiccup. Exhausting the attempts is fatal on purpose.

---

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

`cells` prints **every source the `--src-mask` selects**, because the subject carries
the source code and the two networks issue independent id sequences:

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

The reserved and testing mask bits carry no data and are skipped, since printing them
would list subjects nothing is ever published to.

`history` reads SQLite rather than NATS because a **radius query is not something
subject filtering can express**. `--limit` bounds how many strokes are printed, keeping
the **most recent** ones; omit it for everything in the window. A negative value is
rejected rather than treated as unlimited.

### There is no upstream failover

There is deliberately no failover URL, and `live2` is never dialled automatically. The
two servers issue **independent id sequences** — the same minute showed a last id near
1.48M on one and near 17.8M on the other — so a cursor carried between them is not a
resume but a claim about an unrelated sequence, and the `(src, id)` identity filter
cannot help because colliding ids from different servers name different strokes.

The stored cursor therefore records which server issued it, and on startup:

| Stored cursor | Behaviour |
|---|---|
| from the configured upstream | resumed; no five-minute replay |
| from a **different** upstream | refused, with a warning; starts cold |

Starting cold is safe rather than fatal: the backfill window drops the replay, so the
cost of a repointed `--upstream-url` is one window of backfill, not a pipeline that
will not start. Republishing history as live data — the one failure this whole pipeline
exists to prevent — cannot happen either way. If you want the second server, set
`--upstream-url` to it deliberately and accept the cold start.

Losing the lease to another process **stops the pipeline and exits non-zero**. It has
to: the process can only stop itself by cancelling, and a cancelled pipeline looks
identical to a deliberate shutdown. Exiting 0 would mean a supervisor's
`Restart=on-failure` brought nothing back, and the bridge would have quietly stopped
feeding data while every exit signal said otherwise.

---

## Consuming

Messages are CloudEvents, one per stroke, on
`lightning.v1.src.<n>.cell.<geohash5>`.

**Read through a JetStream consumer, not a plain Core NATS subscription.**
Deduplication applies to the stream, not the subject: a Core subscription sees every
publish including replays. This is asserted in the test suite so it cannot regress
quietly.

```go
js, _ := nc.JetStream()
sub, _ := js.Subscribe("LIGHTNING", nats.Durable("my-consumer"))
for msg := range sub.Messages() {
    var ev feed.Event
    _ = json.Unmarshal(msg.Data, &ev)
    fmt.Println(ev.Data.Lat, ev.Data.Lon, ev.Data.Certainty)
}
```

A region is an arbitrary set of geohash cells, and no single subject filter can name
an arbitrary set of them, so a consumer that wants only your region puts the list on
the **consumer** rather than the subscription. `lightningfeed cells` prints it, and
the demo builds its subscription from the same code path, so the two match by
construction rather than by agreement.

```go
js.Subscribe("",
    handler,
    nats.Durable("my-consumer"),
    nats.BindStream("LIGHTNING"),
    nats.ConsumerFilterSubjects("lightning.v1.src.2.cell.u0xc4", /* … */),
    nats.DeliverNew(),
    nats.AckExplicit(),
)
```

Two things about a durable that catch people out, both of which the demo handles and
both of which are worth knowing:

- **A consumer's configuration is fixed when it is created.** Attaching later does not
  reconfigure it, so a change of delivery policy or subject filter needs a new
  consumer. The demo attaches with `nats.Bind` and refuses to start if the durable is
  filtered to a different region's cells, rather than quietly reporting the old one.
- **`sub.Unsubscribe()` deletes the durable** if the client library created it, not
  just ephemeral ones. Closing the connection is what leaves it in place.

The demo's own notes on this are in `lightningfeed-demo --help`.

### The demo client

`lightningfeed-demo` is a working consumer rather than a snippet: it reads through
JetStream, filters to the radius itself, and draws what it sees.

```bash
# the bridge's own region, from the environment
lightningfeed-demo

# somewhere else, for two minutes
lightningfeed-demo --region-name=munich --region-lat=48.14 --region-lon=11.58 \
    --region-radius-km=25 --duration 2m

# replay what the stream already holds
lightningfeed-demo --replay --duration 30s
```

It reports the certainty breakdown, the closest stroke, how many arrived in the
region's cells but outside the radius, and a rate sparkline. It classifies strokes
itself rather than trusting the `certainty` field in the payload, so its count is
directly comparable with the bridge's — and so it would notice if that field were
ever wrong.

---

## The certainty band

Each stroke carries the upstream's own location-uncertainty estimate (`dev`, median
≈ 2.2 km, observed up to 15 km). At a 10 km radius that makes membership genuinely
**undecidable** for some strokes: one reported 9.9 km out with 2 km of uncertainty may
physically be inside, and one reported 0.5 km out with 15 km of uncertainty may be far
outside.

So every stroke is classified rather than forced into a boolean:

| Band | Rule | Meaning |
|---|---|---|
| `in` | `dist + dev ≤ radius` | certainly inside |
| `boundary` | `dist - dev ≤ radius < dist + dev` | undecidable |
| `out` | `dist - dev > radius` | certainly outside |

`dev` is bounded: a negative figure counts as zero and one above 100 km is capped. `dev`
arrives as an optional integer, so `INT_MAX` — the commonest "no data" sentinel in
this protocol family — is representable, and unclamped it would make every stroke
within `radius + dev` undecidable, which the default policy accepts. One stroke carrying
a sentinel would then publish the whole planet.

**`boundary` strokes count as inside** (`--boundary-policy include`, the default): a
missed stroke is worse than a slightly misplaced one. The band is still carried on every
message and stored per row, so `--boundary-policy exclude` switches to strict
in-circle-only with no rebuild, and `lightningfeed_dropped_total` tells you what it
would cost.

---

## Operational constraints

These are measured against the live upstream, not assumed. The configuration refuses
values that would break them.

**The upstream throttles new connections.**

| Gap between attempts | Successes |
|---|---|
| 0 ms | **0 / 6** |
| 5 s | 2 / 6 |
| 15 s | **6 / 6** |

Failures are silent hangs, not errors. Hence `--reconnect-min` has a **15 s floor**,
enforced at startup, and **exactly one process connects** — the leadership lease
guarantees it. Two ingest instances would spend their lives fighting over a connection
neither could hold.

**The upstream keeps connections open with a bare `{"time":…}` frame roughly every ten
seconds**, so a healthy connection is never idle. `--idle-timeout` (45 s default) is the
watchdog for the case where a connection goes silent without closing.

**The upstream's geo-filter is loose.** A 1.3° × 1.5° request was observed returning
strokes spanning 7.7° × 7.0°, with only ~14% of the live stream landing inside the
requested box. A bounding box is sent with every subscription to reduce its load;
exact filtering is always local.

**Its two transports disagree on types** — the WebSocket sends coordinates as JSON
numbers, the HTTP long-poll fallback as strings. Both are accepted; a plain `float64`
would have silently yielded `0.0` and placed every fallback stroke at the null island.

**A coordinate that is not a coordinate is rejected, not coerced.** `null`, `""` and an
absent field are all dropped per stroke and counted as `bad_coordinate`; a genuine `0` is
a real coordinate and is kept. The absent case is the one that matters: the feed is
undocumented and can change at any time, and a renamed field would otherwise arrive as a
stream of lightning reported off the coast of Africa, with no error anywhere.

[`docs/upstream-findings.md`](docs/upstream-findings.md) is the longer account,
including the measurements behind every number above.

---

## Observability

- `GET :9109/metrics` — Prometheus
- `GET :9109/healthz` — readiness. A standby reports **not ready** on purpose: it is
  working correctly and should not be sent traffic.

Readiness also goes false when the **store stops answering**, and recovers on the next
successful archive write. A failed archive write is otherwise deliberately tolerated — it
costs a missed query rather than a lost stroke — so without this the probe would report
a store that had been failing for an hour as healthy.

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

`upstream_last_message_age_seconds` and `upstream_connected` are deliberately separate.
Age alone cannot distinguish a quiet region from a dead connection, and the connection
gauge cannot survive the moment a socket drops — merging them would have made the one
signal that matters during an outage read as healthy.

`malformed_frames_total` and `rejected_strokes_total` are separate for the same reason,
in the other direction: a frame that decodes but contains one unusable coordinate is
*not* a malformed frame. The decoder rejects the offending stroke and keeps its siblings,
so the two counters answer "was the stream unreadable" and "how much of a readable stream
was unusable" independently.

Alerts worth having: `upstream_last_message_age > 60s` for 5 min;
`dropped/published > 5%`; no leader for 2 min.

The container healthcheck uses `/metrics`, not `/healthz`, because a standby is
*supposed* to report not-ready and marking it unhealthy would be wrong.

### systemd

`deploy/lightningfeed.service` is a hardened unit: unprivileged user, `ProtectSystem=strict`,
no ambient capabilities, and a SIGTERM path that closes the upstream connection and
releases the leadership lease so a standby takes over immediately instead of waiting out
the TTL.

```bash
install -m 0755 lightningfeed /usr/local/bin/
install -m 0644 deploy/lightningfeed.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now lightningfeed
```

`deploy/nats-server.conf` is a matching broker configuration, with a user whose
publish and subscribe permissions are limited to this project's subjects. The compose
file does **not** use it: it runs the broker without credentials and publishes the ports
on loopback only, which is the right default for one machine and the wrong one for
anything shared.

---

## Testing

`make ci` runs the first three of these. `go test ./...` deliberately excludes the
live-upstream tests, which sit behind a build tag.

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

The `e2e` tests are behind a build tag and excluded from `go test ./...` deliberately:
the upstream's connection tolerance is finite, and a suite opening several connections
in sequence consumes everyone else's share. **Do not add live-feed tests to CI.**

Tests run against real infrastructure where it matters: an embedded `nats-server` with
file-backed JetStream, a real SQLite file in a temp directory, and a real WebSocket
server replaying frames captured from the live feed. The protocol decoder's golden
fixtures were captured on 2026-10-04, so protocol drift fails a test instead of
surfacing quietly at 3 a.m. The demo's consumer is tested against a real broker and the
real publisher rather than against events it built itself.

---

## Layout

```
Makefile                   build, test, run and publish
cmd/lightningfeed/        the bridge: ingest, history, cells
cmd/lightningfeed-demo/   the demo consumer
internal/
  geo/                    geohash, haversine, circle cells, certainty bands
  upstream/               protocol decoder and reconnecting WebSocket client
  dedup/                  backfill window and (src,id) suppression
  feed/                   CloudEvent publisher, JetStream stream
  store/                  SQLite cursor and spatial archive
  elect/                  leadership lease
  obs/                    metrics and health
  ingest/                 wiring: the only package that knows the order
  e2e/                    live-upstream tests, behind the e2e tag
  testsupport/            embedded NATS, fake WebSocket, golden fixtures
deploy/                   systemd unit and a broker configuration
docs/upstream-findings.md how the protocol was established, and what was measured
```

---

## Licence

MIT — see [LICENSE](LICENSE). That covers the code only. The lightning data is
© Blitzortung.org contributors under CC BY-SA 4.0, redistributed under the upstream's
terms, and those terms are stricter than the MIT text. See
[Attribution and terms](#attribution-and-terms--read-this-first).
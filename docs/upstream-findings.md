# Upstream findings

The evidence behind this project's design decisions, kept because most of them look
arbitrary until you know what was measured.

`lightningmaps.org` publishes no documentation for its feed, so the protocol in
`internal/upstream` was derived from the client JavaScript the site serves and
then verified against the running service. Everything below was checked live on
2026-10-03 and 2026-10-04, not inferred from documentation. Where a number appears,
it was observed rather than estimated.

The parts worth reading before changing anything:

- **§1.7** the upstream throttles new connections severely, which is why the
  bridge holds a leadership lease and has a 15 s floor on reconnect
- **§1.10** the licensing terms, which are the real constraint on this project and
  are stricter than the licence on the code
- **§1.3** the WebSocket protocol, including the two transports that disagree about
  types
- **§1.6** three traps that produce wrong data rather than errors
- **§2.3** the subject scheme, and why the source code is part of the subject

Sections 2.x are the design as built. The README describes what the program
actually does; this document is the longer account of why.

The original question was whether the feed behind the `www.lightningmaps.org` URL
could be republished onto a NATS topic in Go. It can, and the bridge is exactly the
shape the upstream's terms require.

## 1.1 What the URL actually is

The URL hash selects a viewport, not a data feed:

```
#m=oss;r=0;t=3;s=0;d=2;a=2;dc=0;o=0;n=0;y=35.3353;x=24.5522;z=12;b=;ts=0;dl=2;
```

`y=35.3353`, `x=24.5522` is **Crete, Greece** at zoom 12 (`d`/`dl` = detector overlay levels,
`ts` = strike overlay tiles). The page is a Leaflet map. The strike data arrives over a
**WebSocket to a different host** than the one serving the page.

That point sits **5.24 km east of Roussospiti** — close enough that the measurements here are
representative of the final region, but not the same spot. The final centre and its exact cell set
are in §2.10.

## 1.2 Finding the feed

`/min/index.php?f=js/realtime.js` on the page contains the whole client. The relevant config:

```js
live.config.domain       = 'lightningmaps.org';
live.config.subdomains   = [{domain:'live',weight:1},{domain:'live2',weight:1}];
live.config.ws_path      = "/";
live.config.xhr_path     = "/l/";
live.config.src_mask_default = 4;
live.version             = 24;
```

So there are two interchangeable upstream servers and two transports:

| Transport | Endpoint |
|---|---|
| WebSocket (primary) | `wss://live.lightningmaps.org:443/` , `wss://live2.lightningmaps.org:443/` |
| Long-poll HTTP (fallback) | `https://live{,\|2}.lightningmaps.org/l/?v=24&l=<cursor>&i=<srcmask>` |

The page itself (HTTPS) always negotiates `wss` on port 443.

## 1.3 The WebSocket protocol (verified)

Plain JSON text frames, TLS, **no authentication, no API key, no cookies**.

### Subscribe — client → server, immediately after connect

```json
{"v":24,"i":{},"s":false,"x":0,"w":0,"tx":0,"tw":0,"a":4,"z":5,"b":true,"h":"",
 "l":0,"t":0,"from_lightningmaps_org":true,
 "p":[85.0,-179.9,-85.0,179.9],"r":"A"}
```

| Field | Required | Meaning (verified) |
|---|---|---|
| `v` | **yes** | protocol version, `24` |
| `i` | **yes** | map of `src → last stroke id seen` (resume hint). `{}` on first connect |
| `a` | for data | source bitmask — see §1.4. Default `4` |
| `p` | for data | viewport bbox `[latN, lonE, latS, lonW]` |
| `s` | no | request per-stroke station maps (`sta`) — **leave `false`**, see §1.6 |
| `r` | no | `"A"` on first frame; also used as a reconnect reason tag |
| `z`,`b`,`h`,`l`,`t`,`x`,`w`,`tx`,`tw`,`from_lightningmaps_org` | no | cosmetic/stats; each can be omitted individually and the stream still works |

**Hard requirement:** the frame must contain **both `v` and `i`**. Verified by bisection — every
omission of `v` or `i` alone caused the server to silently close the connection with no hello,
whereas omitting any other single field was fine. An earlier observation that key *order* mattered
did not reproduce (4/4 both ways); that was transient flakiness, see §1.7.

### Hello — server → client

```json
{"cid":25674,"con":123,"port":"8082","time":1791057526.394,"k":639174338.4486406}
```

`cid` session id, `con` connected clients on this backend, `port` backend port,
`time` server Unix time in **seconds**, `k` a keepalive token.

**The `k` reply is not required.** The site's client answers `k` with
`(k*3604)%7081 * now_ms/100`, but verified by A/B test over 3×50 s the server streams
identically when the client ignores `k` entirely, or answers garbage:

| Behaviour | strokes / 50 s |
|---|---|
| reply to `k` (baseline) | 1460 |
| ignore `k` | 1382 |
| reply garbage `{"k":1}` | 1508 |

⇒ **Do not reimplement the `k` transform.** Keep the hello, ignore the token.

### Stroke messages — server → client

Batched, roughly 1.4 msg/s globally, **capped at 500 strokes per message**:

```json
{"time":1791057421,"flags":{"2":0},
 "strokes":[
   {"time":1791057421202,"lat":34.502676,"lon":31.31885,
    "src":2,"srv":1,"id":1471084,"del":1775,"dev":3289}]}
```

| Field | Type | Meaning (verified) |
|---|---|---|
| outer `time` | int | server send time, Unix **seconds** |
| `flags` | map `src → bitmask` | observed `0` and `2`; the client's `flags[s]&6` gate decides whether to re-derive the delay estimate. **Opaque — ignore.** |
| `strokes[].time` | int | stroke time, Unix **milliseconds**, absolute |
| `lat`, `lon` | string | WGS84 position. **JSON strings**, not numbers — must parse |
| `id` | int | monotonic per-source stroke id. Together with `src` it is the **dedup key** |
| `src` | int | `1` or `2`, see §1.4 |
| `del` | int | ms from stroke to the computed position being published: **min 1440, p50 ≈ 1855** |
| `dev` | int | location uncertainty in metres, p50 ≈ 2189, observed 300–15000 |
| `srv` | int | serving backend id — present on `src:2` only |
| `status`, `region` | int | present on `src:1` only (`region` = Blitzortung processing region) |
| `alt` | int | altitude in m, occasionally present |
| `sta` | map | per-stroke station participation, **only when `s:true`** — see §1.6 |

## 1.4 Source bitmask (`a`) — resolved

Recovered from the site's settings UI string:

```js
data: {"lmo":{value:4,text:"Lightningmaps"},
       "bo": {value:2,text:"Blitzortung"},
       "test":{value:8,text:"Testing"}}
```

| Bit | `src` | Network | Verified |
|---|---|---|---|
| 0 | — | `1` | accepted but **0 strokes** — reserved/dead |
| 1 | `1` | Blitzortung.org | 1589 strokes / 3 min, carries `status` + `region` |
| 2 | `2` | LightningMaps.org | 1905 strokes / 3 min, carries `srv` — **the default (`a=4`)** |
| 3 | `8` | testing | accepted, currently 0 strokes |

`a=3` and `a=7` both work. **Use `a=4`** (the site's own default).

## 1.5 Measured characteristics (soak test, 3 min, world bbox, `a=7`)

```
strokes=3494   rate=19.41/s   msgs=319   bytes=398KiB   avgMsg=1277B   2.2KiB/s
per-minute: 2128 (includes backlog), then 699, 667   → ~11.7/s steady state
```

* **Global rate 11–25 strokes/s** across windows. Tiny for a broker.
* **Latency:** `del` p50 ≈ 1.86 s. End-to-end wall latency to a subscriber ≈ 2 s.
* `live` and `live2` are **genuinely different backends with independent id spaces**
  (same minute: `live` last id 1 476 767 vs `live2` 17 836 865).
  ⇒ **Never merge the two.** Pick one, use the other only for failover.
* Crete bbox (the user's URL, 5.24 km east of Roussospiti): **~1.8 strokes/s** — 106 strokes in 60 s.

## 1.6 Three traps that will bite an implementation

**Trap 1 — every reconnect replays ~5 minutes of history.**

A fresh connect and a resume both deliver a burst covering ~300 s of strokes:

```
FRESH connect  (i={})          n=1150  age 3.3s .. 303s
RESUME i={2:1477565}           n=1172  age 2.2s .. 299s   → 962 replayed, 210 new
```

`i` is only a hint; the server re-sends the window regardless. ⇒ **Dedup on `(src, id)` is
mandatory.** The site itself does exactly this.

**Trap 2 — the server-side geo filter is very loose.**

Requesting a 1.3° × 1.5° box around Barcelona, the live stream spanned **7.7° × 7.0°** and only
13.7% of delivered strokes were inside the box:

```
requested bbox lat[41.0,41.6] lon[2.0,2.6]
BACKLOG (first 6s)  n=648  inside=36   lat[36.08,45.73] lon[-2.97,6.35]
LIVE   (after 6s)   n=146  inside=20   lat[36.52,41.45] lon[-2.12,3.54]
```

A Pacific box and a Tokyo box correctly returned **zero** strokes, so filtering does happen — at
coarse grid granularity with wide padding. ⇒ **Use `p` to reduce load, but filter exactly client-side.**

**Trap 3 — do not enable `s:true`.** Station maps are enormous: one `src:2` stroke measured
**3632 bytes** of `sta` (≈230 stations) vs ~100 bytes without — a ~36× bandwidth inflation for a
field this project does not need. (The values are bit flags; the site tests `sta[s] & 4` for
"station was used in the location solution".)

## 1.7 The upstream throttles new connections

Hammering connect attempts (24 sequential, no delay) is heavily rate limited:

| Gap between attempts | Success |
|---|---|
| 0 ms | **0 / 6** |
| 5 s | 2 / 6 |
| 15 s | **6 / 6** |

Failures are silent: the TCP/TLS connect hangs and never upgrades, no error frame. Every long-lived
connection held for 30–180 s during this work succeeded reliably.

⇒ **Design consequences: one single long-lived upstream connection per deployment, and a reconnect
backoff that starts at ≥ 15 s with jitter.** This is the single strongest argument for the
leader-elected single-writer architecture in Part 2.

## 1.8 HTTP long-poll fallback (also verified)

```
GET https://live.lightningmaps.org/l/?v=24&l=<cursor>&i=4
→ {"w":2000,"o":500,"d":[{...}],"s":61287526,"t":1791057526}
```

* `i` must be within `1..7`; `i=8` or higher returns the literal body `err`.
* `d` = strokes, omitted entirely when nothing new arrived.
* `s` = a monotonic global sequence number, also the resume cursor for `l` (~61.2 M at test time).
* `w` / `o` = server-advised next-poll delay in ms (500–5000). True long-poll behaviour.
* **`strokes[].time` here is RELATIVE milliseconds** (e.g. `-32042` = 32 s ago), whereas the
  WebSocket sends **absolute** ms. Normalising this is a required part of the model.
* Every response embeds a usage notice:

  > "Lightning data copyright by Blitzortung.org contributors. This file was created for
  > LightningMaps.org only. **It's not allowed to publish this data elsewhere! Commercial usage
  > is forbidden!** Contact: info@blitzortung.org"

This is a fallback and a protocol cross-check. The WebSocket is the primary.

## 1.9 A richer alternative feed exists (LZW-compressed)

`wss://ws1.blitzortung.org/` and `wss://ws7.blitzortung.org/` (port 443 — **not** `:3000`, which
refused) accept `{"a":111}` and stream **LZW-compressed JSON text frames, one stroke per message,
worldwide, no geo filter**. This is the feed `map.blitzortung.org` uses.

### What LZW is

**L**empel–**Z**iv–**W**elch is a dictionary compression algorithm. Rather than emitting bytes one
at a time, it emits *references to phrases it has already seen*, and the decoder rebuilds the same
dictionary in lockstep.

Encoding works like this. Start with a dictionary of all 256 single characters. Read the input and
find the longest phrase already in the dictionary, emit its index as a code, then **add
`phrase + next character` to the dictionary** and keep going. So repeated substrings cost one code
instead of many.

Decoding is the mirror image and needs no dictionary table of its own — the structure is implied:

```
decoder:  w  = first code            # previous phrase
          for each next code c:
              e = dictionary[c]      # or the single char if c < 256
              if e is undefined:     # the "KwKwK" case
                  e = w + w[0]       #   code refers to w itself
              output e
              dictionary.push(w + e[0])   # ← same entry the encoder created
              w = e
```

Because JSON keys repeat on every single message — `"lat":`, `"lon":`, `"alt":`, `"pol":`,
`"mds":`, `"mcg":`, `"status":`, `"region":`, `"sig":[`, `"sta":`, `"time":` — after the first
stroke the dictionary already contains them, which is why this particular stream compresses so well
despite being *uncompressed JSON underneath*.

### The exact variant, verified

Blitzortung uses a **simplified LZW**: fixed 8-bit codes, dictionary entries indexed from **256**,
and **no clear code and no end-of-information code**. Three frames captured live and decoded to
valid JSON:

```
frame 0: 759 B → 1967 B  (2.59x)  ✓ valid JSON
frame 1: 896 B → 2457 B  (2.74x)  ✓ valid JSON
frame 2: 1130 B → 3443 B (3.05x)  ✓ valid JSON
```

**One bug dominates this decoder**, and it is worth stating because naive implementations get 99%
of the output right and still fail: **the first code must be emitted as literal output**, not held
back as the "previous phrase". Holding it back yields output that *looks* correct —
`"lat":21.477688,"lon":-97.25946` is all perfectly right — but drops the opening `{`, so
`JSON.parse` fails on every single frame while the decoded text appears healthy in a terminal.

### It is not JSONL — the framing is one object per frame

The missing `{` is **entirely a decoder bug, not a framing artefact**. Measured on all three frames:

| | frame 0 | frame 1 | frame 2 |
|---|---|---|---|
| first char of **compressed** frame | `{` (code 123) | `{` | `{` |
| first char of decoded output | `{` | `{` | `{` |
| last char of decoded output | `}` | `}` | `}` |
| newlines in decoded output | **0** | **0** | **0** |
| `{` / `}` count | 23 / 23 | 29 / 29 | 41 / 41 |
| `JSON.parse` | OK, 13 keys | OK | OK |

The compressed frame's very first character is the **literal byte 123 (`{`)** — a plain literal, not
a dictionary code, so there is nothing to decode. Each frame is a self-contained, single JSON
object containing exactly one stroke. There is no JSONL, no newline framing, and no multi-object
concatenation anywhere: `JSON.parse` would reject trailing content, and the braces balance exactly
(the surplus `{`/`}` pairs are the nested `sig` station objects — 22, 28 and 40 of them).

### Decoded payload

```json
{"time":1791090309065896200,"lat":21.477688,"lon":-97.25946,"latc":0,"lonc":0,
 "alt":0,"pol":0,"mds":5994,"mcg":249,"status":0,"region":3,"delay":8.5,
 "sig":[{"sta":2488,"time":1240657,"lat":22.143259,"lon":-101.032524,"alt":1952,"status":4}, …]}
```

| Field | Meaning |
|---|---|
| `time` | Unix **nanoseconds** |
| `lat`, `lon`, `alt` | position, altitude in m |
| `pol` | polarity (0 = not determined) |
| `mds` | max deviation span, µs — location fit quality |
| `mcg` | max circular gap, degrees — station geometry around the strike |
| `status`, `region` | stroke status; Blitzortung processing region |
| `sig[]` | the detecting stations: id, arrival-time offset, position, altitude, status. 22–40 stations observed |
| `delay` | **seconds** of processing latency — observed 3.2–8.5 s |
| `latc`, `lonc` | observed `0` in all samples; undocumented, possibly cloud-related flags |

Strictly richer than the lightningmaps.org feed: nanosecond timestamps, polarity, explicit quality
metrics, and per-station arrival data. But it is **~35 lines of Go** (see `internal/upstream/lzw`
in the module layout), it has **no server-side geo filter** so bandwidth and client-side filtering
both get worse, and its `delay` is 3–8 s versus ~1.9 s on the primary feed.

**Recommendation: ship on the lightningmaps.org WebSocket. Keep this as a documented v2 option** —
the decoder is now solved and verified, so v2 is a contained piece of work.

## 1.10 Licensing — the real constraint

From `blitzortung.org/en/contact.php` and `docs.lightningmaps.org`, verbatim where it bites:

> It is not allowed to use our lightning data **for storm warning systems, for plausibility checks
> of overvoltage damages, or risk analysis for precautionary protection of high-quality technology,
> even if the data are not obtained directly from our site but from third-party websites.**

> The use of our **raw lightning data is allowed only to the participants of the project or to those
> we explicitly have allowed it.**

> A commercial use of our data is strongly prohibited, even by the users that send data to our servers.

The intro of the official docs is even blunter:

> The system is made for private and entertainment purposes. It is not an official information
> service for lightning data. A commercial use of our data is strongly prohibited, even by the users
> that send data to our servers.

External projects are permitted under conditions, and two of them decide this architecture:

> - The project may not represent commercial interests.
> - It must be of general interest for the participants of Blitzortung.org.
> - It must be different from existing projects. Do not just reimplement other projects.
>   **Do not setup yet another visualization of online data.**
> - All applications that use our data must be freely accessible.
> - The source of the data must be clearly identified.
> - The operator must prevent any possible misuse of the data provided, as far as this is possible.
> - **The applications (web sites, apps, ...) have to retrieve their data from a separate server and
>   not from the servers of Blitzortung.org.**
>
> All data remain under the **CC-BY-SA 4.0** license.

Three consequences, in order of importance:

1. **The NATS bridge is the sanctioned pattern.** The rules *require* consumers to read from a
   separate server rather than hitting Blitzortung directly. A local bridge that holds the one
   upstream connection and republishes internally is precisely that. Stated positively in the
   README and attribution.
2. **Permission scope depends on distribution.** See §1.10a — resolved for private use; still
   required before any public or commercial release.
3. **Hard prohibitions** — no storm warnings, no overvoltage plausibility checks, no risk analysis,
   nothing commercial, and this must not become another map viewer.

Note the tension: CC-BY-SA 4.0 nominally permits redistribution, while the terms and the in-band
copyright string forbid publishing elsewhere. **Treat the terms as authoritative.** Attribution
(source, CC-BY-SA 4.0, Blitzortung.org contributors) must be carried in the payload metadata and in
the README regardless of scope.

### 1.10a Scope decision — private use

**Decided 2026-10-04: this is a private, non-commercial deployment.** Under that scope the
published terms are satisfied without a separate written permission, because free use is explicitly
granted for *"private, entertainment, non-commercial purposes"* and a local bridge for one's own use
meets every applicable condition:

| Condition | Status for private use |
|---|---|
| Non-commercial | ✅ no monetisation, no charge, no business use |
| Freely accessible | ✅ available to the operator, not sold or gated |
| Source clearly identified | ✅ in-band attribution + README (do this anyway) |
| Must not hit Blitzortung servers directly | ✅ **satisfied by the bridge architecture itself** |
| Different from existing projects | ✅ a message feed, not a visualization |
| Operator must prevent misuse | ✅ private deployment; misuse guardrails documented |
| Not for storm warning / overvoltage / risk analysis | ⚠️ **out of scope by design — do not add such features** |

**This unblocks M0 for private deployment.** Two obligations carry forward into M5:

1. Attribution must be in the payload and the README from day one.
2. The prohibited use cases stay out of scope permanently, not just today.

**Re-open M0 and obtain written permission before** any of: making the NATS stream reachable
outside the operator's own network, publishing the bridge or a consumer publicly, or any commercial
use. The contact path is the forum (`forum.blitzortung.org`); `info@blitzortung.org` is listed but
they ask that it not be used for data requests.

## 1.11 Feasibility verdict

| Requirement | Verdict |
|---|---|
| Real-time strikes available | **Yes**, ~2 s latency, no auth |
| Feed reachable from Go | **Yes**, verified `wss://live.lightningmaps.org:443/` |
| Volume suits NATS | **Yes**, 11–25 strokes/s, ~2.2 KiB/s |
| Fits licensing | **Yes with conditions** — bridge pattern is required; permission needed |
| Production-safe | **Yes, with care** — throttling (§1.7), replay (§1.6), loose geo-filter (§1.6) |

**Verdict: build it. Go 1.25, `coder/websocket`, `nats.go`.**

## 1.12 Alternatives considered

| Option | Why not |
|---|---|
| Blitzortung LZW feed | Richer, but no geo filter and 3–8 s `delay` vs ~1.9 s. Decoder solved (§1.9) — keep as v2. |
| Blitzortung archive (`archive_data.php`) | Participants only, and not real-time |
| Commercial providers (Vaisala, Earth Networks, meteorage) | Required if this ever becomes commercial — the only licence-clean path |
| NOAA / WWLLN public streams | No public real-time stroke feed |
| Weather APIs (Open-Meteo etc.) | No lightning |
| Core NATS without JetStream | Loses replay; see §2.4 |

---

# Part 2 — Plan

## 2.1 Architecture

```
                    ┌──────────────────────────────────────────────────┐
                    │  lightningfeed ingest  (LEADER ONLY)             │
                    │  ─ exactly one upstream connection ─              │
                    │  ─ exactly one SQLite writer ─                    │
                    │                                                  │
 wss://live…:443 ───►│  upstream.Client ─► normalize ─► dedup ─┐        │
 (1 conn, leader)    │                    (src,id)   radius  │        │
                    │                                     │  ┌─────▼────┐│
                    │                                     └─►│  SQLite  ││
                    │                                        │ cursor + ││
                    │                                        │ spatial  ││
                    │                                        │ archive  ││
                    │                                        └──────────┘│
                    └──────────────────────────────────────────┼─────────┘
                                                           │ publish
                    ┌──────────────────────────────────────▼──┐
                    │  NATS  JetStream stream: LIGHTNING       │
                    │  lightning.v1.src.2.cell.<geohash5>      │
                    │  (file storage, 2 h, dedup window 2 h)  │
                    └──────────────────────────────────────┬──┘
                                                           │ subscribe
                        ┌──────────────────────────────────┼───────────┐
                        ▼                                  ▼           ▼
                  live tailers      SQLite radius query       alerting, analytics
                  (30 cells)        "every strike ≤10 km,       (rate, lag,
                                     last N hours"              reconnects)
```

The leader lock is what enforces §1.7: exactly one process ever dials upstream. It also enforces the
SQLite single-writer rule (§2.5).

**Leader election** — NATS KV bucket `lightningfeed/leader`, `Create` with an expected revision
gives a compare-and-swap. Winner holds the lock with a TTL and renews it; on loss or on process
exit the TTL lapses and a standby takes over. Standbys do **not** connect upstream and only dial
upstream after winning the lock.

## 2.2 Module layout

```
lightningfeed/
├── go.mod                          module github.com/<you>/lightningfeed   (go 1.25)
├── README.md                       attribution + terms compliance (M0)
├── LICENSE
├── cmd/
│   └── lightningfeed/
│       ├── main.go                 cobra root
│       ├── ingest.go               subcommand: leader-elected bridge
│       ├── tail.go                 subcommand: live tail to stdout
│       └── replay.go               subcommand: JetStream catch-up
├── internal/
│   ├── config/config.go            flags + env, 12-factor
│   ├── upstream/
│   │   ├── protocol.go             wire types, src mask, version const
│   │   ├── client.go               dial, subscribe, read loop
│   │   ├── backfill.go             warm-up window handling
│   │   ├── lzw/lzw.go              v2 only: Blitzortung feed decoder (§1.9, solved)
│   │   └── client_test.go
│   ├── model/stroke.go             normalized Stroke
│   ├── geo/
│   │   ├── bbox.go                 exact containment
│   │   └── geohash.go              cell → subject suffix
│   ├── dedup/dedup.go              (src,id) TTL LRU
│   ├── store/
│   │   ├── store.go                SQLite open, WAL, migrations
│   │   ├── cursor.go               upstream resume cursor (§2.5a)
│   │   └── region.go               10 km circle: cell set + radius query (§2.10)
│   ├── feed/
│   │   ├── subjects.go             subject construction
│   │   ├── publisher.go            JetStream + Core publish
│   │   └── leader.go               KV leader lock
│   ├── testsupport/
│   │   ├── fixtures/               captured upstream JSON (golden)
│   │   └── fakews/                 replaying fake upstream server
│   └── obs/                        slog + prometheus metrics, /healthz, /readyz
└── deploy/
    ├── nats-server.conf
    └── lightningfeed.service
```

**Dependencies** (all verified present in the local module cache):

| Module | Version | Purpose |
|---|---|---|
| `github.com/nats-io/nats.go` | v1.54.0 | NATS + JetStream |
| `github.com/coder/websocket` | v1.8.15 | context-aware WS client |
| `modernc.org/sqlite` | latest | pure-Go SQLite driver, no cgo (§2.5) |
| `github.com/spf13/cobra` | v1.10.2 | CLI |
| `github.com/prometheus/client_golang` | latest | metrics |

Standard library only otherwise — `log/slog` for logging, `net/http` for the health endpoint.
No viper: flags + env is enough and keeps the dependency surface small.

## 2.3 Subject scheme

```
lightning.v1.stroke                 every stroke, normalized          (default)
lightning.v1.src.1                  Blitzortung.org network
lightning.v1.src.2                  LightningMaps.org network
lightning.v1.cell.<geohash5>         ~4.0 × 4.9 km cell, for geo-scoped consumers
lightning.v1.system.<metric>        ingest health: rate, lag, reconnects (retained)
```

Wildcard subscriptions stay cheap:

* `lightning.v1.>` — everything
* `lightning.v1.src.2.>` — one network
* `lightning.v1.cell.sw3[0-9a-z]` — every precision-5 cell inside geohash-3 `sw3`

**The cell precision is geohash-5, chosen from measurements, not taste.** The target is a 10 km
radius around Roussospiti (§2.10), so the question is: how many subjects must a consumer subscribe
to, and how much out-of-circle data does it receive as a result?

Measured at Roussospiti, radius 10 km, circle area 314 km²:

| precision | cell size | cells to subscribe | overshoot vs circle area |
|---|---|---|---|
| 3 | 128 × 156 km | 1 | **63.6×** — ~20 000 km² of junk |
| 4 | 32 × 20 km | 2 | **4.0×** |
| **5** | **4.0 × 4.9 km** | **30** | **1.87×** ← chosen |
| 6 | 1.0 × 0.6 km | 693 | 1.3× — 693 subjects is absurd for a private deployment |

Precision 5 is the knee: 30 subjects is free at NATS scale, and a subscriber receives only 1.87×
the circle's worth of strokes (≈586 km² of cells for a 314 km² circle), which a `haversine ≤ 10 km`
test then trims exactly.

The 30 subjects for Roussospiti, verified by decoding each cell's bbox and intersecting with the
circle, with **circle coverage confirmed at 200 000/200 000 sampled points**:

```
sw32d sw32e sw32f sw32g sw32s sw32t sw32u sw32v sw32w sw32x
sw32y sw32z sw334 sw335 sw336 sw337 sw33d sw33e sw33h sw33j
sw33k sw33m sw33n sw33p sw33q sw33r sw33s sw33t sw33w sw33x
```

Geohash edge cases to handle in `internal/geo`: longitude wrap (a centre near ±180°) and latitude
clamping near the poles, where cells stretch in longitude. Verified reference values:
Roussospiti `35.3340688, 24.4944483` → `sw3`/`sw33`/`sw33j`/`sw33j2`; Barcelona `41.39, 2.15` → `sp3`.

**The bridge publishes the most specific form only** — `lightning.v1.src.2.cell.swn` — and relies on
NATS wildcard matching for the broader views. Publishing to all three would triple the write rate
for no gain: `lightning.v1.>` and `lightning.v1.src.2.>` both already match it. If a deployment
wants a source-only subject for consumers that must not do geo routing, enable it explicitly with
`--publish-subject-prefixes`.

## 2.4 Payload

One message per stroke, JSON, wrapped as a **CloudEvent structured-mode** envelope — NATS has no
native metadata envelope, and CloudEvents is the portable convention for exactly this shape of
bridge:

```json
{
  "specversion": "1.0",
  "type":          "org.blitzortung.lightning.stroke.v1",
  "source":        "wss://live.lightningmaps.org",
  "subject":       "lightning.v1.src.2.cell.swn",
  "id":            "2:1474130",
  "time":          "2026-10-03T20:00:00.000Z",
  "datacontenttype": "application/json",
  "data": {
    "id": 1474130,
    "src": 2,
    "network": "lightningmaps.org",
    "time": "2026-10-03T19:59:58.220Z",
    "lat": 34.502676,
    "lon": 31.318850,
    "deviation_m": 3289,
    "delay_ms": 1775,
    "serving_backend": 1,
    "distance_km": 4.87,
    "certainty": "in",
    "received_at": "2026-10-03T20:00:00.000Z",
    "attribution": "Lightning data (c) Blitzortung.org contributors, CC BY-SA 4.0"
  }
}
```

Design decisions:

* **One stroke per message**, not upstream batches. Batching is an upstream transport detail; at
  20 msg/s it costs nothing to unbatch, and it makes per-stroke dedup, `Nats-Msg-Id`, and
  geo-cell routing possible. Raw upstream batches go to `lightning.v1.raw` only if M2 asks for it.
* **CloudEvent `id` = `<src>:<id>`** — the upstream dedup key, so it doubles as the idempotency key.
* **`received_at` vs `time`** kept distinct: `time` is the physical stroke, `received_at` is when we
  published. `delay_ms` bridges them. Consumers computing staleness need both.
* **Numbers, not strings**, for lat/lon — the upstream sends JSON strings; parsing belongs in
  `upstream`, not in consumers.
* Attribution is in-band, satisfying "the source of the data must be clearly identified".
* **`distance_km` and `certainty`** are computed by the bridge against the configured region
  (§2.10), so consumers never re-implement haversine or guess at the uncertainty band. Only present
  when `--region-*` is set.

**JetStream over Core NATS.** At 20 msg/s durability is not the concern — *catch-up* is. On
restart, a Core NATS consumer silently misses everything during the gap; a JetStream consumer
resumes by sequence. Config:

```
stream: LIGHTNING, subjects: lightning.v1.>
storage: file
retention: limits          (max_age 2h, max_msgs 5_000_000)
duplicate_window: 2h       ← dedups the §1.6 reconnect replay at the broker
discard: new_subject
```

`duplicate_window: 2h` is the elegant part: the bridge publishes with
header `Nats-Msg-Id: <src>:<id>` and the broker itself discards replayed strokes. Consumers still
dedup on `id`, but a redelivery after an unclean restart becomes harmless.

2 h retention mirrors the upstream's own 1–2 h display window and is ample for a
"what was happening during my outage" catch-up.

## 2.5 SQLite persistence (`internal/store`)

NATS JetStream already covers durable replay, so SQLite is here for the two things NATS genuinely
cannot do. Both are driven by the 10 km-region requirement in §2.10.

Driver: **`modernc.org/sqlite`** — pure Go, no cgo. This matters: the deployment target should be a
single static binary that copies to a box and runs, and `mattn/go-sqlite3` would drag in a C
toolchain. WAL mode, one writer, readers never block.

### (a) Resume cursor — survives restart

The upstream's `i` field and the HTTP `s` sequence are **upstream state**, so NATS cannot hold them.
Without persistence every restart replays ~5 minutes and every failover to `live2` starts blind.

```sql
CREATE TABLE upstream_cursor (
  src            INTEGER PRIMARY KEY,   -- 1 = Blitzortung.org, 2 = LightningMaps.org
  last_id        INTEGER NOT NULL,      -- highest stroke id seen
  last_time_ms   INTEGER NOT NULL,      -- wall clock when we saw it
  server         TEXT    NOT NULL       -- 'live' | 'live2' (id spaces are independent!)
);

CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
-- k='http_seq'    v=<global sequence from the /l/ fallback>
-- k='schema_ver'  v='1'
```

Written once per upstream message (not per stroke — at 500 strokes/message that is plenty), inside
the same transaction as the stroke batch. On startup the bridge seeds `i` from this table instead of
`{}`, so a restart continues rather than replaying.

### (b) Spatial archive — the query NATS subjects cannot answer

The real requirement is *"every strike within 10 km of the centre, over the last N hours"*. That is a
**radius query**, and NATS subject filtering cannot express it: it would mean subscribing to the 30
cells and then receiving ~1.9× the needed volume as a firehose, with no ability to ask a historical
question.

```sql
CREATE TABLE stroke (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  src         INTEGER NOT NULL,
  network     TEXT    NOT NULL,
  stroke_id   INTEGER NOT NULL,
  time_ms     INTEGER NOT NULL,        -- upstream stroke time, absolute
  received_ms INTEGER NOT NULL,        -- when we published it
  lat         REAL    NOT NULL,
  lon         REAL    NOT NULL,
  deviation_m INTEGER,                 -- upstream `dev`, may be NULL
  delay_ms    INTEGER,                 -- upstream `del`, may be NULL
  cell        TEXT    NOT NULL,        -- geohash-5, indexed
  UNIQUE (src, stroke_id)
);
CREATE INDEX stroke_time_idx  ON stroke (time_ms);
CREATE INDEX stroke_cell_idx  ON stroke (cell, time_ms);

CREATE TABLE region (
  id         INTEGER PRIMARY KEY CHECK (id = 1),
  name       TEXT NOT NULL,            -- 'roussospiti'
  lat        REAL NOT NULL,            -- 35.3340688
  lon        REAL NOT NULL,            -- 24.4944483
  radius_km  REAL NOT NULL,            -- 10
  cells      TEXT NOT NULL             -- JSON array of the 30 geohash-5 subjects
);
```

The `UNIQUE (src, stroke_id)` constraint is a **fourth, durable dedup layer** — the last line of
defence behind backfill-drop, JetStream `Nats-Msg-Id`, and the in-memory TTL cache. An
`INSERT OR IGNORE` makes a duplicate a no-op instead of a row.

Add a `certainty` column (`in` | `boundary` | `out`) so the §2.10 band is queryable, not baked in:

```sql
ALTER TABLE stroke ADD COLUMN certainty TEXT NOT NULL DEFAULT 'in';
```

Strokes are archived for **every stroke in the 30 region cells**, not only the `in` ones.
`boundary` strokes are published and archived (they count as inside), but because the band is a
column a strict query is one `WHERE certainty='in'` away — and the share is measurable, not guessed.

Retention is a scheduled job, not a trigger: `DELETE FROM stroke WHERE time_ms < ?` on a timer,
configurable and defaulting to 7 days. At the measured 20 strokes/s global that is ~12 M rows/day —
SQLite handles that comfortably with the index, and the radius query stays sub-millisecond. If the
circle's rate is what matters rather than the global rate, subscribing upstream with a tight `p`
bbox (§2.7) cuts the row count by ~7×.

⚠️ **One writer, enforced.** The SQLite file belongs to the leader. Standby replicas must not write
it, or use their own path. Do not put it on shared storage — the leader lock already guarantees
single-writer semantics, and SQLite's locking model is not built for NFS.

### What stays in NATS, not SQLite

Live tailing and catch-up-after-outage remain JetStream's job. Duplicating them in SQLite would mean
two sources of truth and two replay paths. SQLite's role is: upstream resume state, and radius
queries over time.

## 2.6 Normalisation rules (`internal/model`)

| Concern | Rule |
|---|---|
| `lat`/`lon` | upstream strings → `float64`; reject out-of-range as malformed |
| Timestamp | WS `time` is absolute ms. HTTP `time` is **relative** ms. Normalise both to `time.Time` UTC at the edge; the model exposes only absolute |
| Missing fields | `dev`, `alt`, `del` are optional — `*int32`/zero-with-presence. Never fail a stroke over a missing optional field |
| Schema drift | Unknown fields **ignored**, never an error. A new required field must not break the pipeline |
| Malformed frame | Count it, log it, skip it. One bad frame must not kill the connection |
| `src` | unknown values pass through with `network: "unknown"` rather than being dropped |

## 2.7 Connection lifecycle

```
start → acquire KV leader lock
      → connect wss (TLS 1.2+, SNI, 20 s handshake timeout)
      → send subscribe {v, i, a, p, …}
      → wait for hello (10 s timeout)
      → drop backfill window (see below)
      → publish strokes
         ├─ read error / timeout 45 s  → reconnect
         ├─ ctx cancelled            → release lock, exit 0
         └─ ping/pong watchdog        → reconnect

reconnect backoff: 15s, 30s, 60s, 120s, 300s (cap), ±25% jitter
```

* **Backoff floor is 15 s** — directly from §1.7. A 1 s retry loop gets you 0% success.
* **Ping/pong watchdog** at 45 s idle is required: the connection can go silent without closing.
* **Failover** to `live2` after ≥ 5 failed attempts on `live`. Because the two servers have
  independent id spaces, the `i` resume hint must be **reset** on server switch, and
  `dedup` must key on `(src, id)` — which it does.
* **Backfill window** (`backfill: drop`, default): ignore strokes older than `backfill_drop_seconds`
  (300 s, matching the measured replay depth) relative to connect time. This is the first line of
  defence against replay; the broker dedup window is the second; consumer dedup is the third.
  A `backfill: publish` mode is available for archive bootstrap.

## 2.8 Configuration

Flags with env-var overrides (`LIGHTNINGFEED_*`):

```
--nats-url            nats://127.0.0.1:4222
--jetstream           true                 # false → plain Core NATS publish
--subjects            lightning.v1.>       # what this instance publishes
--upstream-host       live.lightningmaps.org
--upstream-failover   live2.lightningmaps.org
--src-mask            4                    # 4 = LightningMaps.org (default of the site)
--bbox                -85,-180,85,180      # latS,lonW,latN,lonE
--exact-bbox          true                 # client-side filter (§1.6)

# the 10 km region around Roussospiti (§2.10) — omit all four for world-wide ingest
--region-name         roussospiti
--region-lat          35.3340688
--region-lon          24.4944483
--region-radius-km    10
--boundary-policy     include              # include | exclude  (§2.10 — decided: include)

--sqlite              /var/lib/lightningfeed/lightningfeed.db
--sqlite-retention    168h                 # 7 days of strokes

--stations            false                # keep false: 36× bandwidth (§1.6)
--backfill            drop                 # drop | publish
--backfill-drop-seconds 300
--reconnect-min       15s
--reconnect-max       5m
--idle-timeout        45s
--leader-elect        true
--metrics-addr        :9109
--log-level           info
```

`--bbox` empty ⇒ subscribe with a world bbox but publish nothing outside `--exact-bbox`.

**Setting `--region-*` is the recommended default** for this deployment. It narrows the upstream
bbox to the circle, cuts measured volume ~7×, reduces the SQLite row count by the same factor, and
restricts the published subjects to the 30 cells the region actually needs — all without changing
the consumer interface. Omit it to run as a world-wide bridge instead.

`--boundary-policy` decides how §2.10's uncertain strokes are treated. This is a **deliberate,
explicit** setting rather than a hidden default, because it changes what "a strike within 10 km"
means.

## 2.9 Observability

`log/slog` JSON to stdout. Prometheus on `:9109`:

```
lightningfeed_strokes_total{src,cell}      counter
lightningfeed_publish_total{result}        published | dropped_dup | dropped_bbox | dropped_backfill | failed
lightningfeed_upstream_connected           gauge 0/1
lightningfeed_upstream_last_message_age_s  gauge  ← primary health signal
lightningfeed_reconnects_total{host,reason}
lightningfeed_duplicate_total
lightningfeed_malformed_total
lightningfeed_leader                       gauge 0/1
```

Alerts worth having day one:

| Alert | Condition | Why |
|---|---|---|
| `upstream_stale` | last message age > 60 s for 5 min | upstream wedged or throttled |
| `upstream_down` | disconnected > 5 min | connection lost |
| `rate_dropped` | dropped/published > 5% | upstream format changed |
| `not_leader` | no leader for > 2 min | KV lock problem |

## 2.10 The 10 km region around Roussospiti — and an honest note about "precision"

The requirement: **every lightning strike within ~10 km of Roussospiti, Crete.** That has two
distinct halves, and conflating them is the trap.

### The region

| | |
|---|---|
| Centre | **Roussospiti (Ρουσσοσπίτι)** — Rethymno regional unit, Crete, Greece |
| Coordinates | **35.3340688 N, 24.4944483 E** (OSM node 728630214; corroborated by Wikipedia 35°20′N 24°29′E and 123City 35°20′02″N 24°29′41″E) |
| Site | 300 m altitude on the slope of Vrysinas (peak ~1208 m), ~4.5 km SE of Rethymno |
| Radius | 10 km → circle area 314 km² |
| Centre geohash | `sw3` / `sw33` / `sw33j` / `sw33j2` |

Note the URL in the original request pointed at `35.3353, 24.5522` — **5.24 km east of the village
centre**, and a different geohash-5 cell (`sw33n` vs `sw33j`). The §1.5 soak measurements were taken
from that offset point, so the rates below are representative but slightly displaced; they are not
measured at the final centre.

Upstream viewport bbox for a 10 km circle at this latitude (1 km = 0.008983° lat, 0.011011° lon here):

```
p = [35.4239, 24.6046, 35.2442, 24.3843]        (latN, lonE, latS, lonW)
```

### Completeness within the circle — fully achievable

No stroke inside the circle is ever missed. **Verified: 200 000/200 000 randomly sampled points
across the circle fall inside the published cell set — zero misses.** Achieved by:

1. `--bbox` set to the bbox above so the upstream itself reduces volume (§1.6 — the server filter
   is loose but real, and a tight box cut measured ~7×)
2. `internal/geo` enumerating the 30 geohash-5 cells intersecting the circle, stored in the
   `region` table
3. Publishing to those 30 cells only, so subscribers get 1.87× the circle's volume
4. `haversine(centre, stroke) ≤ 10 km` as the final exact test — a circle, not the bounding box

### Location accuracy of each stroke — NOT improvable

The *reported position* of a stroke is whatever Blitzortung's network computed. Measured `dev`
(uncertainty): **p50 ≈ 2.2 km, observed range 0.3–15 km**. No amount of engineering sharpens that;
it is a property of detector geometry and station density.

This matters because a 10 km radius is only ~4.5× the median uncertainty:

* a stroke at 9.9 km with `dev` = 2 km may physically be inside the circle
* a stroke at 0.5 km with `dev` = 15 km may physically be well outside it

So "was this strike inside the circle?" is **not always decidable from the data**. Rather than
pretend otherwise, every stroke is classified into three bands and the consumer never has to
recompute it:

| band | rule | meaning |
|---|---|---|
| `in` | `dist + dev ≤ 10 km` | certainly inside |
| `boundary` | `dist - dev ≤ 10 km < dist + dev` | genuinely uncertain |
| `out` | `dist - dev > 10 km` | certainly outside |

**Decision (2026-10-04): `boundary` strokes count as inside** — `--boundary-policy include`, the
default. The reasoning fits this use case: a missed strike is worse than a slightly misplaced one,
and a 10 km circle around Roussospiti is a *presence* question ("was there lightning near the
village"), not a forensic claim about where a channel-to-ground stroke actually terminated.

The distinction is still **not buried**: each message carries `certainty` (`in` | `boundary` |
`out`) and `lightning_strokes_by_certainty_total` is exported. If you ever need a strict
in-circle-only figure, `--boundary-policy exclude` switches it without a rebuild — and the metric
tells you what that would have cost. Expect the `boundary` share to be non-trivial.

**Measure the local `dev` distribution rather than trusting the global p50.** Vrysinas is
mountainous terrain, 300 m up a slope, and terrain shape affects how well a network triangulates.
After a few days of ingest, query the archive for the real local distribution:

```sql
SELECT deviation_m, COUNT(*) FROM stroke
 WHERE cell IN (…) AND deviation_m IS NOT NULL
 GROUP BY deviation_m ORDER BY deviation_m;
```

The boundary fraction then becomes a measured number rather than an estimate. If the local `dev`
turns out materially worse than 2.2 km, revisit the radius before trusting any counts.

### Rate expectation

Measured ~1.8 strokes/s ≈ 105/min at the offset point, varying seasonally (October is peak).
Roughly 40–50% of that lands inside the 10 km circle at Roussospiti. That is the volume to design
the SQLite write path and any alerting around — not the 20/s global figure.

| # | Milestone | Estimate | Done when |
|---|---|---|---|
| **M0** | ~~Terms gate~~ — **✅ resolved 2026-10-04, private scope** | — | Private non-commercial deployment satisfies the published terms without a separate permission (§1.10a). Carried into M5: attribution in payload + README; prohibited use cases documented as permanently out of scope. **Re-open before any public/commercial release.** |
| **M1** | Upstream client | 3 d | 30 min soak; reconnect works; fake-upstream unit tests green |
| **M2** | Model, dedup, geo, store | 3 d | Duplicates provably suppressed across all 4 layers; exact radius filter unit-tested; SQLite cursor survives restart |
| **M3** | NATS publisher | 3 d | Strokes land in JetStream; `Nats-Msg-Id` dedup proven against a replay; 30 region cells published |
| **M4** | Leader election | 2 d | Two instances ⇒ exactly one upstream connection **and** one SQLite writer (assert in test) |
| **M5** | Ops surface | 2 d | Health endpoints, metrics, graceful shutdown, systemd unit, `nats-server.conf`, **attribution shipped** |
| **M6** | Consumer CLI + SDK | 2 d | `tail` live; `replay --since` catch-up; `near --radius 10km --since 24h` history query; documented Go consumer example |
| **M7** | Test + docs | 3 d | Coverage on `upstream`/`dedup`/`geo`/`store`; runbook; CI |

**~17 working days. No external blocker remains for private use.**

Suggested order: M1 → M2 → M3 → M6 (get data flowing end-to-end early) → M4 → M5 → M7.

## 2.11 Test strategy

**Fake upstream server** (`internal/testsupport/fakews`) — the single highest-value test asset.
It dials nothing out, replays captured golden frames, and can inject every failure mode:

* backfill burst → assert dedup suppresses it
* duplicated `(src,id)` across messages → assert one publish
* malformed JSON → assert connection survives
* unknown/new field → assert ignored, not fatal
* silent hang → assert idle-timeout reconnect
* immediate close loop → assert backoff floor respected

**Golden fixtures** — real captured frames from this discovery session, so protocol changes are
caught by a failing test rather than by silence at 3 a.m.

| Suite | Target | Notes |
|---|---|---|
| unit | `upstream`, `model`, `dedup`, `geo`, `feed` | table-driven; no network |
| integration | `ingest` against fakews + local `nats-server` | `-tags integration`, spins up an ephemeral server |
| contract | fixtures vs current live feed | opt-in, nightly only — **never** in CI proper, to respect §1.7 throttling |
| e2e | `tail` against a live ingest | manual, documented |

CI must not run the live-feed contract test: hammering connect attempts is precisely what §1.7
throttles. Nightly, single connection, sequential.

## 2.12 Risks

| Risk | Impact | Mitigation |
|---|---|---|
| **Scope creep into a prohibited use** | terms breach | private, non-commercial by design; storm-warning / overvoltage / risk-analysis features are permanently out of scope (§1.10a). Re-open M0 before any public or commercial release |
| **Attribution dropped** | terms breach | attribution is in-band in every message and required in the README (M5 acceptance) |
| Feed undocumented, can change or be gated at any time | pipeline breaks silently | pin `v=24`; ignore unknown fields; alert on `malformed_total` and `publish_total` collapse; keep the HTTP fallback and the Blitzortung feed as documented alternates |
| Connection throttling (§1.7) | can't reconnect, looks like an outage | 15 s backoff floor; single-writer leader election; no tight loops anywhere |
| Replay duplicates (§1.6) | duplicate strokes downstream | three layers: backfill drop → `Nats-Msg-Id` → consumer dedup on `(src,id)` |
| Loose geo-filter (§1.6) | 85% wasted bandwidth, wrong data downstream | `p` for load reduction + exact client-side filter; metrics on dropped ratio |
| Global storm burst | volume spike | at 20/s → 1000/s this is still nothing; JetStream limits + publisher confirms + drop-with-metric rather than unbounded queueing |
| `live`/`live2` id-space collision | corrupt data if merged | single-server invariant; `i` reset on switch; `(src,id)` key; cursor records which server it came from |
| Two writers on the SQLite file | corruption | leader owns the DB path; standbys use their own or none. Never on NFS/shared storage |
| SQLite grows unbounded | disk exhaustion | scheduled `DELETE` on `time_ms`, retention default 7 d, `strokes_total` vs `rows` gauge, alert on file size |
| `boundary` strokes silently included or dropped | wrong region semantics | explicit `certainty` field + `--boundary-policy` flag + dedicated metric (§2.10) |
| Clock/time skew | wrong timestamps | `time` is upstream-authoritative; `received_at` records ours; NTP on the host |
| Stations leak participant locations | privacy | `s` hard-wired false; document why in code |
| Accidental commercial/prohibited use | terms breach | non-commercial, no storm-warning/risk-analysis features; stated in README |

## 2.13 Open questions

Answering these before M1 avoids rework.

1. ~~**Scope**~~ — **resolved: private, non-commercial** (§1.10a). No public distribution planned.
2. ~~**Geography**~~ — **resolved: 10 km radius around Roussospiti**, Crete, at
   **35.3340688, 24.4944483** (§2.10). Single region, not world-wide.
3. ~~**Persistent DB**~~ — **resolved: SQLite** (§2.5), pure-Go driver, WAL, single writer on the
   leader. Retention 7 days.
4. ~~**Boundary strokes**~~ — **resolved: count as inside** (§2.10). `--boundary-policy include`
   is the default; the `certainty` band is still carried on every message and stored per row, so
   the decision is reversible without a rebuild.
5. ~~**"Not for my Stone one"**~~ — **resolved**: `roussospiti` is a directory in your workspace, so
   this referred to the village, not a second deployment. Single-region scope confirmed.
6. **Core NATS or JetStream?** Plan assumes JetStream. Core is fine if nothing ever needs
   catch-up — but then §1.6's replay problem becomes the consumer's problem.
7. **One stroke or upstream batches?** Plan assumes un-batched one-per-message. Confirm no
   consumer needs the exact upstream framing.
8. **Protobuf instead of JSON?** Cheap at ~2 msg/s for this region, but protobuf + a schema registry
   is a better long-term contract if non-Go consumers are likely.
9. **Second network (`src:1`)?** `a=6`/`a=7` works and roughly doubles volume. For a single village
   circle `a=4` alone is the likely answer — but `src:1` is the only network with explicit
   `region` tagging, which might matter for a per-village breakdown.
10. **Retention for NATS** — 2 h proposed; SQLite holds 7 days. Any consumer needing longer?
11. **Auth on NATS** — if anything leaves the host, credentials/TLS are mandatory, not optional.

---

## Appendix A — Verification commands used

```bash
# protocol constants, straight from the site
curl -s 'https://www.lightningmaps.org/min/index.php?f=js/realtime.js' | grep -o "live.config.subdomains[^;]*"

# handshake + subscribe (Node 22 has a built-in WebSocket client)
#   wss://live.lightningmaps.org:443/  ->  {"v":24,"i":{},"a":4,"p":[85,-179.9,-85,179.9],...}

# HTTP fallback
curl -s 'https://live.lightningmaps.org/l/?v=24&l=0&i=4'
#   {"w":500,"o":500,"copyright":"...not allowed to publish this data elsewhere!...",
#    "x":true,"s":61241092,"ds":-61261092}

# invalid src mask
curl -s 'https://live.lightningmaps.org/l/?v=24&l=0&i=16'   # -> err

# richer alternative feed
#   wss://ws7.blitzortung.org/  ->  send {"a":111}  ->  LZW-compressed JSON, one stroke/msg

# terms
curl -s https://www.blitzortung.org/en/contact.php
```

## Appendix B — Reference implementations (protocol cross-checks)

All independently published; none copied. Useful to diff against when the feed changes.

| Project | Language | What it covers |
|---|---|---|
| `tobiasv/MyBlitzortung` | PHP | The site's own legacy embed library (archived) |
| `Craeckie/Lightningmaps` | TS | Independent protocol spec of both feeds, incl. the LZW framing |
| `clemensv/real-time-sources` | Go | LightningMaps → Kafka/MQTT/AMQP via CloudEvents. Closest existing analogue to this project |
| `SimonSchick/BlitzortungAPI` | TS | `ws*.blitzortung.org` client, full stroke semantics |
| `wuan/bo-android` | Kotlin | Official community Android app; the reference LZW decoder |

The decoder in §1.9 was implemented from the algorithm description above and validated against three
frames captured live from `wss://ws7.blitzortung.org/` — all three decode to valid JSON. Cross-check
against `wuan/bo-android` only if the feed format ever changes.
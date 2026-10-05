// Command lightningfeed-demo consumes the stream this project publishes.
//
// It exists because "the bridge is running" and "a consumer can read the strokes"
// are different claims, and only the second one is worth anything to somebody
// building on top of it. Everything here goes through the public path — JetStream,
// the CloudEvent envelope, the published subjects — so a demo that works is
// evidence that a real consumer will work too.
//
// Configuration is shared with the bridge on purpose. It reads the same
// LIGHTNINGFEED_* environment variables and the same region flags, so the demo
// can never disagree with the publisher about which region is in force. A demo
// pointed at a different circle would simply report an empty region, which from
// the outside is indistinguishable from a broken bridge.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"

	"github.com/mheers/nats-lightning/internal/config"
	"github.com/mheers/nats-lightning/internal/feed"
	"github.com/mheers/nats-lightning/internal/geo"
)

// Defaults for the demo's own settings.
const (
	// defaultDurable names the JetStream consumer this demo reads through.
	//
	// It is a durable rather than an ephemeral subscription on purpose.
	// Deduplication lives on the stream and not on the subject, so a plain Core
	// subscription would see every replay as well as every live stroke; and a
	// durable is what lets the demo survive its own Ctrl-C without losing the
	// position it had reached.
	defaultDurable = "lightningfeed-demo"

	// defaultFade is how long a stroke stays on the map.
	//
	// Lightning is not a slow phenomenon. A cell that lit up three minutes ago
	// says where the storm has been, not where it is, and leaving old strokes on
	// screen turns the map into a history rather than a picture of now.
	defaultFade = 90 * time.Second

	// refreshEvery is how often the map is redrawn.
	refreshEvery = time.Second

	// bucketEvery and bucketCount build the rate sparkline: one bucket per ten
	// seconds across the last five minutes.
	bucketEvery = 10 * time.Second
	bucketCount = 30

	// maxMarkers bounds the strokes held for the map.
	//
	// A busy region produces strokes faster than the map can usefully show them,
	// and an unbounded slice here would be a slow leak in a program whose entire
	// purpose is to be left running for hours.
	maxMarkers = 4000

	// quietHintAfter is when a demo that has seen nothing says so.
	//
	// Silence is the normal state of this feed: a region with no storm produces
	// nothing for hours. A demo that waits silently is therefore
	// indistinguishable from one that is broken, and saying something once the
	// silence has gone on long enough to be meaningful is the difference between
	// a demo and a hang.
	quietHintAfter = 30 * time.Second
)

// options are the demo's own settings. They are deliberately not part of the
// bridge's configuration, where they would mean nothing.
type options struct {
	view     string
	duration time.Duration
	limit    int
	durable  string
	replay   bool
	fade     time.Duration
}

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := newRoot().ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "lightningfeed-demo: %v\n", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	opts := options{}

	cmd := &cobra.Command{
		Use:   "lightningfeed-demo",
		Short: "Watch the lightning strokes this project publishes",
		Long: strings.TrimSpace(`
Reads the bridge's JetStream stream and draws the strokes as they arrive.

It reads through a JetStream consumer rather than a plain Core NATS
subscription, because deduplication applies to the stream and not to the subject:
a Core subscription sees every republished stroke as well as every live one.

Region settings come from the same flags and the same LIGHTNINGFEED_* variables
as the bridge, so both processes always agree on which region is in force.

` + feed.Attribution + `

Lightning data is licensed for private, non-commercial use only, and must not be
used for storm warning. See "Attribution and terms" in the README.`),
		SilenceUsage: true,
		// main prints the error itself, prefixed with the program name. Without
		// this cobra prints it first as well, so every failure appeared twice.
		SilenceErrors: true,
		// The bridge's flags are defined by internal/config rather than by cobra, so
		// cobra is told to hand its arguments over untouched.
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Flag parsing is disabled, so cobra sees neither --help nor --version and
			// the flag package would print the bridge's usage instead: no demo flags
			// at all, and nothing about the map.
			if wantsHelp(args) {
				fmt.Fprint(cmd.OutOrStdout(), helpText)
				return nil
			}
			if wantsVersion(args) {
				fmt.Fprintf(cmd.OutOrStdout(), "lightningfeed-demo %s\n", buildVersion())
				return nil
			}
			rest, err := extractDemoFlags(args, &opts)
			if err != nil {
				return err
			}
			return run(cmd.Context(), rest, opts)
		},
	}

	cmd.SetHelpFunc(func(c *cobra.Command, _ []string) {
		fmt.Fprint(c.OutOrStdout(), helpText)
	})

	return cmd
}

const helpText = `
Watch the lightning strokes this project publishes.

Usage:
  lightningfeed-demo [demo flags] [bridge flags]

Demo flags:
  --view map|log      map draws a terminal map that redraws in place; log prints
                      one line per stroke (default map; a non-terminal stdout
                      always gets lines, whatever this says)
  --duration 5m       stop after this long instead of running until interrupted
  --limit 100         stop after this many strokes; omit for no limit
  --durable NAME      JetStream consumer to read through (default ` + defaultDurable + `)
  --replay            start at the beginning of the stream rather than from now
  --fade 90s          how long a stroke stays on the map

The durable keeps its position between runs, so a second run continues where the
first stopped instead of replaying two hours of stream. --replay only applies when
the durable is first created: a consumer's settings are fixed at creation, so a
durable created with --replay keeps replaying from the start. Use a different
--durable, or delete the consumer, to change that:

  nats consumer rm LIGHTNING lightningfeed-demo --force

A durable is also filtered to one region's cells, fixed when it was created. Point
this at a different region and the demo refuses rather than quietly reporting the
old one.

Bridge flags, each with a LIGHTNINGFEED_* equivalent that wins over it:
  --nats-url          NATS server URL                 LIGHTNINGFEED_NATS_URL
  --stream            JetStream stream name            LIGHTNINGFEED_STREAM
  --region-name       region name, for labels         LIGHTNINGFEED_REGION_NAME
  --region-lat        region centre latitude          LIGHTNINGFEED_REGION_LAT
  --region-lon        region centre longitude         LIGHTNINGFEED_REGION_LON
  --region-radius-km  region radius in km             LIGHTNINGFEED_REGION_RADIUS_KM
  --src-mask          which networks to read          LIGHTNINGFEED_SRC_MASK
  --log-level         debug, info, warn or error      LIGHTNINGFEED_LOG_LEVEL
  --log-format        json or text                    LIGHTNINGFEED_LOG_FORMAT

Omit all three --region-* flags to watch the whole world. Supplying only some of
them is an error rather than a silent world-wide feed, so a typo cannot quietly
turn a regional demo into a global one.

This demo builds its subscription from the same cell list the bridge publishes to,
so the subjects match by construction rather than by agreement. To see that list,
or to build a subscription of your own, ask the bridge:

  lightningfeed cells --region-name=... --region-lat=... --region-lon=... \
      --region-radius-km=...

Examples:
  # same region as the bridge, straight from the environment
  docker compose exec lightningfeed lightningfeed-demo --view map

  # a specific place, for two minutes
  lightningfeed-demo --region-name=munich --region-lat=48.14 --region-lon=11.58 \
      --region-radius-km=25 --duration 2m

  # pipe it somewhere: log lines, no map
  lightningfeed-demo --view log > strokes.log
`

// wantsHelp reports whether the arguments are asking for the usage text.
//
// Handled here rather than by cobra, because flag parsing is disabled and cobra
// therefore cannot recognise a help flag that arrives in the argument list.
func wantsHelp(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "-h", "--help", "help":
			return true
		}
	}
	return false
}

// wantsVersion reports whether the arguments are asking for the build version.
func wantsVersion(args []string) bool {
	for _, arg := range args {
		switch arg {
		case "-v", "--version", "version":
			return true
		}
	}
	return false
}

// buildVersion reports the version, preferring the module version recorded by the
// Go toolchain and falling back to the value stamped with -ldflags at build time.
func buildVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}

// extractDemoFlags pulls the demo's own flags out of the argument list.
//
// They are stripped before the shared configuration parser sees them, because it
// would reject them as unknown. This is the same approach the bridge uses for its
// history-only flags and for the same reason: one parser owns the bridge's
// configuration, and a second parser for the demo would let the two drift apart on
// what a region flag means.
func extractDemoFlags(args []string, opts *options) ([]string, error) {
	var rest []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, inline, hasInline := strings.Cut(arg, "=")

		if !strings.HasPrefix(arg, "-") {
			rest = append(rest, arg)
			continue
		}

		// value pulls the flag's argument, accepting both spellings the standard
		// flag package accepts: "--flag value" and "--flag=value".
		value := func() (string, error) {
			if hasInline {
				return inline, nil
			}
			if i+1 >= len(args) {
				return "", fmt.Errorf("%s needs a value", name)
			}
			i++
			return args[i], nil
		}

		if name == "--replay" {
			if !hasInline {
				opts.replay = true
				continue
			}
			b, err := strconv.ParseBool(inline)
			if err != nil {
				return nil, fmt.Errorf("--replay %q: %w", inline, err)
			}
			opts.replay = b
			continue
		}

		switch name {
		case "--duration", "--fade", "--limit", "--durable", "--view":
		default:
			rest = append(rest, arg)
			continue
		}

		v, err := value()
		if err != nil {
			return nil, err
		}
		if err := applyDemoFlag(opts, name, v); err != nil {
			return nil, err
		}
	}
	return rest, nil
}

func applyDemoFlag(opts *options, name, v string) error {
	switch name {
	case "--duration", "--fade":
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("%s %q: %w", name, v, err)
		}
		// Zero and negative are sign errors rather than "no limit". A demo that
		// exited immediately, or one that never exited, would both look like a bug
		// in something nobody wrote.
		if d <= 0 {
			return fmt.Errorf("%s %v: must be positive", name, d)
		}
		if name == "--duration" {
			opts.duration = d
		} else {
			opts.fade = d
		}
	case "--limit":
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s %q: %w", name, v, err)
		}
		if n < 0 {
			return fmt.Errorf("--limit %d: must not be negative; omit it for no limit", n)
		}
		opts.limit = n
	case "--durable":
		if v == "" {
			return fmt.Errorf("--durable needs a name")
		}
		opts.durable = v
	case "--view":
		if v != "map" && v != "log" {
			return fmt.Errorf("--view %q: want map or log", v)
		}
		opts.view = v
	}
	return nil
}

func run(ctx context.Context, rest []string, opts options) error {
	if opts.view == "" {
		opts.view = "map"
	}
	if opts.durable == "" {
		opts.durable = defaultDurable
	}
	if opts.fade == 0 {
		opts.fade = defaultFade
	}

	cfg, err := config.ParseFromEnv(rest)
	if err != nil {
		return err
	}

	// A piped stdout gets lines rather than a map.
	//
	// Redrawing in place needs cursor control, and a file or a pipe has no cursor:
	// the escape sequences would end up in the log as noise, and the map's entire
	// value is that it replaces itself.
	if !isTerminal(os.Stdout) {
		opts.view = "log"
	}

	subjects, err := subjectsFor(cfg)
	if err != nil {
		return err
	}

	// startedAt is taken once and used for both the elapsed-time readouts and the
	// rate histogram, so the two cannot disagree about when the demo began.
	started := time.Now()

	r := &reader{
		out:       os.Stdout,
		view:      opts,
		region:    cfg.Region,
		startedAt: started,
		limit:     opts.limit,
		byBand:    map[geo.Certainty]int{},
		byNet:     map[string]int{},
		closestKm: math.Inf(1),
		buckets:   make([]int, bucketCount),
		bucketAt:  started,
	}

	nc, err := nats.Connect(cfg.NATSURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.ErrorHandler(r.asyncError),
	)
	if err != nil {
		return fmt.Errorf("connecting to NATS at %s: %w", cfg.NATSURL, err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("JetStream is unavailable at %s: %w", cfg.NATSURL, err)
	}

	delivery := nats.DeliverNew()
	if opts.replay {
		delivery = nats.DeliverAll()
	}

	// The subject list goes on the consumer rather than on the subscription.
	//
	// A region is an arbitrary set of geohash cells, and no single subject filter
	// can name an arbitrary set of them, so this is what the server-side filter
	// exists for. It requires an empty subscription subject and an explicit stream
	// binding, which is why both are set.
	//
	// An existing consumer is attached to rather than reconfigured.
	//
	// Sending a configuration for a consumer that already exists fails whenever any
	// field differs, and the delivery policy is the field that differs most: a run
	// with --replay creates the durable with "deliver all", so every later run
	// without it would be refused with a message about a deliver policy nobody
	// mentioned. Bind attaches to the consumer as it stands and sends no
	// configuration at all, which also makes the durable's own settings — its
	// position and its filter — the ones that decide what arrives.
	existing, lookupErr := js.ConsumerInfo(cfg.Stream, opts.durable)
	attaching := lookupErr == nil && existing != nil

	// The subscription handle is discarded on purpose. The subscription lives as
	// long as the connection does, and there is nothing to do with it that would not
	// delete the durable: see the note below.
	if attaching {
		if err := checkFilter(existing, subjects, cfg.Stream, opts.durable); err != nil {
			return err
		}
		_, err = js.Subscribe("", r.handle, nats.Bind(cfg.Stream, opts.durable))
	} else {
		_, err = js.Subscribe("",
			r.handle,
			nats.Durable(opts.durable),
			nats.BindStream(cfg.Stream),
			nats.ConsumerFilterSubjects(subjects...),
			delivery,
			nats.AckExplicit(),
		)
	}
	if err != nil {
		// Almost always the stream does not exist, which means the bridge has not
		// run or is pointed elsewhere. Saying so is more use than a broker error
		// naming a stream the reader has never heard of.
		if strings.Contains(err.Error(), "stream not found") {
			return fmt.Errorf(
				"no stream %q at %s: start the bridge first (docker compose up -d lightningfeed), "+
					"or point --stream at the one it publishes to", cfg.Stream, cfg.NATSURL)
		}
		return fmt.Errorf("subscribing to %d subject(s): %w", len(subjects), err)
	}
	// Deliberately no sub.Unsubscribe().
	//
	// nats.go marks a subscription for consumer deletion when the library created
	// the consumer itself, and Unsubscribe then removes it — durables included, not
	// just ephemerals. So unsubscribing here would delete this demo's own durable on
	// every exit, and the next run would start from the beginning of the stream again
	// while appearing to resume. Closing the connection, which the deferred nc.Close
	// does, leaves the consumer and its position in place.

	if opts.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.duration)
		defer cancel()
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The limit is enforced by cancelling rather than by unsubscribing, so that
	// every message already delivered is acked and the durable's position is left
	// consistent instead of being abandoned part-way through a delivery.
	r.onLimit = cancel
	r.header(cfg, subjects, attaching, existing)

	r.render(runCtx)

	// Closing the connection is what stops delivery, and it is deferred above. No
	// Unsubscribe: see the note where the subscription is created.
	cancel()
	r.summary()
	return nil
}

// checkFilter refuses to attach to a durable that is filtered to a different
// region's cells.
//
// A consumer's subject filter is fixed when it is created, and attaching to it
// inherits that filter rather than the one asked for now. So changing the region
// and re-running does not change what arrives: the demo keeps delivering the old
// cells, which presents as a bridge that has stopped. The two honest ways out are a
// fresh durable or deleting the old consumer, and naming them is cheaper than
// letting somebody work it out from an empty screen.
//
// The error is returned rather than warned about because continuing would mean
// reporting the numbers as though they described the region that was asked for.
func checkFilter(info *nats.ConsumerInfo, want []string, stream, durable string) error {
	got := info.Config.FilterSubjects
	if len(got) == 0 {
		// A consumer with a plain subscription subject rather than a filter list,
		// which is what a consumer created outside this command looks like. There is
		// nothing to compare, and nothing here can tell what it will receive.
		return fmt.Errorf(
			"consumer %q on stream %q is not filtered to subject list, so its region cannot be "+
				"verified against this one.\nUse a different --durable, or delete it and run again:\n"+
				"  nats consumer rm %s %s --force",
			durable, stream, stream, durable)
	}

	wantSet := make(map[string]struct{}, len(want))
	for _, s := range want {
		wantSet[s] = struct{}{}
	}
	gotSet := make(map[string]struct{}, len(got))
	for _, s := range got {
		gotSet[s] = struct{}{}
	}
	if len(wantSet) == len(gotSet) {
		same := true
		for s := range wantSet {
			if _, ok := gotSet[s]; !ok {
				same = false
				break
			}
		}
		if same {
			return nil
		}
	}

	return fmt.Errorf(
		"consumer %q on stream %q is filtered to %d subject(s) that are not this region's %d.\n"+
			"A consumer's filter is fixed when it is created, so changing the region does not change it.\n"+
			"Use a different --durable, or delete this one and run again:\n"+
			"  nats consumer rm %s %s --force",
		durable, stream, len(got), len(want), stream, durable)
}

// subjectsFor lists the subjects a consumer of this configuration needs.
//
// With a region configured these are exactly the cells the region covers, one
// subject per selected network — which is why the network is part of the subject
// at all: the two networks issue independent id sequences, so a subscriber that
// dropped the source would be merging two unrelated id spaces. This is the same
// list `lightningfeed cells` prints, so nobody has to reimplement the cell
// enumeration in order to subscribe correctly.
func subjectsFor(cfg *config.Config) ([]string, error) {
	cells, err := cfg.Cells()
	if err != nil {
		return nil, fmt.Errorf("enumerating region cells: %w", err)
	}
	if len(cells) == 0 {
		// World-wide: there is no cell list, so subscribe to the whole namespace.
		return []string{feed.SubjectPrefix + ".>"}, nil
	}

	sources := cfg.SourceMask.Sources()
	if len(sources) == 0 {
		return nil, fmt.Errorf(
			"src-mask %d selects no network that carries data; "+
				"2 is Blitzortung.org, 4 is lightningmaps.org", int(cfg.SourceMask))
	}

	subjects := make([]string, 0, len(cells)*len(sources))
	for _, src := range sources {
		for _, cell := range cells {
			subjects = append(subjects,
				fmt.Sprintf("%s.src.%d.cell.%s", feed.SubjectPrefix, int(src), cell))
		}
	}
	// Sorted so the header and any error message list them in a stable order.
	sort.Strings(subjects)
	return subjects, nil
}

// reader accumulates what arrives and renders it.
//
// Every field is guarded by mu except during construction, because the render
// loop reads the totals while the consumer goroutine is still writing them.
type reader struct {
	out  *os.File
	view options

	region    geo.Circle
	limit     int
	onLimit   context.CancelFunc
	startedAt time.Time

	mu       sync.Mutex
	inside   int // strokes the region test accepted
	filtered int // published, but outside the radius
	undecod  int
	byBand   map[geo.Certainty]int
	byNet    map[string]int
	buckets  []int
	bucketAt time.Time

	lastAt      time.Time
	closestKm   float64
	haveClosest bool

	markers  []marker // newest first
	pending  []string // log lines waiting for the next frame
	reported atomic.Bool

	printed int // lines the last frame occupied, for in-place redraw
}

// asyncError reports a subscription-level failure once.
//
// Connection-level errors are left to nats.go, which already retries them and
// logs them where it logs; this exists for the failures a reader would otherwise
// never learn about, such as a consumer that the server has stopped delivering
// to. It prints once rather than per event because the alternative is a screen
// that fills with the same line while the demo is unusable anyway.
func (r *reader) asyncError(_ *nats.Conn, sub *nats.Subscription, err error) {
	if sub == nil || !r.reported.CompareAndSwap(false, true) {
		return
	}
	fmt.Fprintf(r.out, "\nconsumer problem: %v\n", err)
}

// handle is the subscription callback: it decodes one message and records it.
//
// It runs on nats.go's own dispatch goroutine, rather than being fed from a
// channel. A channel would need a buffer, and a buffer that fills during a burst
// makes the client drop messages and declare itself a slow consumer — a demo that
// silently loses strokes exactly when there are most of them. The shared state it
// touches is behind the mutex the render loop also takes.
//
// The acknowledgement is unconditional. A message that fails to decode is still
// acknowledged, because redelivering it would fail identically and would park the
// consumer behind it for ever.
func (r *reader) handle(msg *nats.Msg) {
	defer func() { _ = msg.Ack() }()

	var ev feed.Event
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		r.mu.Lock()
		r.undecod++
		r.mu.Unlock()
		return
	}
	r.record(ev)
}

// record applies the region test and updates the totals.
func (r *reader) record(ev feed.Event) {
	now := time.Now()
	lat, lon := ev.Data.Lat, ev.Data.Lon

	// The coordinates are checked rather than trusted. The payload is a bridge
	// away, and a nonsensical coordinate would quietly plot in the map instead of
	// being reported, which is the same failure the upstream decoder goes to some
	// length to prevent on the way in.
	if math.IsNaN(lat) || math.IsNaN(lon) || math.IsInf(lat, 0) || math.IsInf(lon, 0) ||
		lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		r.mu.Lock()
		r.undecod++
		r.mu.Unlock()
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.lastAt = now
	r.bucket(now)

	dev := 0
	if ev.Data.DeviationM != nil {
		dev = *ev.Data.DeviationM
	}

	// The region test is run again here, and that is not redundant. The bridge
	// publishes every stroke in a cell that overlaps the region, so a stroke as
	// much as one cell beyond the radius arrives rather than being dropped, and it
	// is this test that catches it. Filtering only on the published subject would
	// count those. The filtered tally is worth showing because it is precisely what
	// lightningfeed_dropped_total{reason="outside_region"} means on the bridge.
	//
	// The certainty band is the demo's own classification whenever a region is
	// configured, not the one the bridge published, even though the bridge's is
	// right there in the payload.
	//
	// Taking the published band while filtering on the demo's own radius test would
	// leave the two answering different questions: the count would be totalled under
	// the bridge's verdict while membership was decided locally, and a disagreement
	// between them would show up as a sum that does not add up. One test, one
	// verdict — which also means the demo's total matches the bridge's published
	// count for the same region, so the two can be compared directly.
	band := ev.Data.Certainty
	dist := math.NaN()
	if r.region.RadiusKm > 0 {
		var computed geo.Certainty
		dist, computed = r.region.ClassifyPoint(lat, lon, float64(dev))
		band = computed

		if !r.region.Contains(lat, lon, float64(dev), geo.PolicyInclude) {
			r.filtered++
			r.appendLine(formatStroke(ev, dist, band))
			return
		}
		if dist < r.closestKm {
			r.closestKm = dist
			r.haveClosest = true
		}
	} else if band == "" {
		// World-wide: nothing knows about a region, so no band was published and
		// there is nothing to classify. Every stroke is equally in.
		band = geo.CertaintyIn
	}

	r.inside++
	r.byBand[band]++
	r.byNet[networkOf(ev)]++
	r.markers = append([]marker{{lat: lat, lon: lon, at: now}}, r.markers...)
	r.trim(now)
	r.appendLine(formatStroke(ev, dist, band))

	if r.limit > 0 && r.inside >= r.limit && r.onLimit != nil {
		r.onLimit()
	}
}

func networkOf(ev feed.Event) string {
	if ev.Data.Network != "" {
		return ev.Data.Network
	}
	return fmt.Sprintf("src.%d", ev.Data.Src)
}

// formatStroke renders one stroke as a log line.
func formatStroke(ev feed.Event, distKm float64, band geo.Certainty) string {
	dev := "absent"
	if ev.Data.DeviationM != nil {
		dev = fmt.Sprintf("%dm", *ev.Data.DeviationM)
	}
	delay := "absent"
	if ev.Data.DelayMS != nil {
		delay = fmt.Sprintf("%dms", *ev.Data.DelayMS)
	}

	line := fmt.Sprintf("%s  src=%d id=%-9d %8.4f,%8.4f  dev=%-7s delay=%-7s %s",
		ev.Data.Time, ev.Data.Src, ev.Data.ID, ev.Data.Lat, ev.Data.Lon, dev, delay, networkOf(ev))
	if !math.IsNaN(distKm) {
		line += fmt.Sprintf("  %7.2f km  %s", distKm, band)
	}
	return line
}

// trim drops markers that have faded and enforces the budget.
func (r *reader) trim(now time.Time) {
	kept := r.markers[:0]
	for _, m := range r.markers {
		if now.Sub(m.at) <= r.view.fade {
			kept = append(kept, m)
		}
	}
	r.markers = kept
	if len(r.markers) > maxMarkers {
		r.markers = r.markers[:maxMarkers]
	}
}

// appendLine queues a log line for the next frame.
func (r *reader) appendLine(line string) {
	if r.view.view != "log" {
		return
	}
	r.pending = append(r.pending, line)
}

// bucket counts the stroke into the rate histogram.
//
// Whole buckets are advanced rather than indexing by elapsed time, so the row is
// a fixed window: a gap of several minutes scrolls off the left rather than
// leaving the row half empty.
func (r *reader) bucket(now time.Time) {
	// A reader built without a histogram — a test, or a caller that does not want
	// one — must not spin here. Without a bucketAt the first comparison is against
	// the zero time, which is always far enough in the past to satisfy it.
	if r.bucketAt.IsZero() {
		return
	}
	for now.Sub(r.bucketAt) >= bucketEvery {
		r.bucketAt = r.bucketAt.Add(bucketEvery)
		if len(r.buckets) > 0 {
			r.buckets = append(r.buckets[1:], 0)
		}
	}
	if n := len(r.buckets); n > 0 {
		r.buckets[n-1]++
	}
}

// render redraws until ctx ends.
func (r *reader) render(ctx context.Context) {
	ticker := time.NewTicker(refreshEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.draw(now)
		}
	}
}

// draw paints one frame.
func (r *reader) draw(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.view.view == "map" {
		proj := newProjection(r.region, terminalWidth())
		lines := r.statusLines(now)
		lines = append(lines, "")
		lines = append(lines, renderMap(proj, r.markers, now, r.view.fade)...)
		lines = append(lines, "")
		lines = append(lines, "  "+sparkline(r.buckets)+
			fmt.Sprintf("  strokes per %s, most recent on the right", bucketEvery))
		r.repaint(lines)
		return
	}

	// Log view. Lines are flushed in batches rather than one write per stroke, so
	// that a burst does not become a burst of syscalls, and so that everything the
	// demo received is on stdout even if it is stopped between two frames.
	if len(r.pending) == 0 {
		return
	}
	pending := r.pending
	r.pending = nil
	for _, line := range pending {
		fmt.Fprintln(r.out, line)
	}
}

// header prints what is being watched, before any data arrives.
//
// attaching says whether an existing durable was reused. It is worth saying out
// loud, because a demo that attaches to a durable created with --replay is reading
// from the beginning of the stream whatever its own flags say, and a header that
// did not mention it would claim a delivery policy the run is not using.
func (r *reader) header(cfg *config.Config, subjects []string, attaching bool, existing *nats.ConsumerInfo) {
	where := "world-wide"
	if cfg.Region.RadiusKm > 0 {
		where = fmt.Sprintf("%s  %.4f,%.4f  within %.4g km",
			cfg.RegionName, cfg.Region.Lat, cfg.Region.Lon, cfg.Region.RadiusKm)
	}

	delivery := "from now"
	switch {
	case attaching:
		// The consumer's own policy wins over this run's flags, because the policy
		// was fixed when it was created and cannot be changed by attaching to it.
		delivery = fmt.Sprintf("resumed, position kept by consumer %q", r.view.durable)
		if existing != nil {
			delivery += fmt.Sprintf(" (created to deliver %s)", deliverPolicyName(existing.Config.DeliverPolicy))
		}
	case r.view.replay:
		delivery = "from the start of the stream"
	}

	fmt.Fprintf(r.out, "watching %s\n", where)
	fmt.Fprintf(r.out, "stream %q at %s\n", cfg.Stream, cfg.NATSURL)
	fmt.Fprintf(r.out, "%d subject(s), consumer %q, %s\n", len(subjects), r.view.durable, delivery)
	if r.view.view == "map" {
		fmt.Fprintf(r.out, "map %dx%d, strokes fade after %s\n\n",
			newProjection(cfg.Region, terminalWidth()).cols, mapRows, r.view.fade)
	}
	fmt.Fprintf(r.out, "%s\n\n", feed.Attribution)
}

// deliverPolicyName renders a policy as something worth reading.
//
// The numeric value is not: it is a broker-side enum, and a header claiming
// "delivery 0" tells the reader nothing about whether the demo is about to replay
// two hours of stream.
func deliverPolicyName(p nats.DeliverPolicy) string {
	switch p {
	case nats.DeliverAllPolicy:
		return "everything from the start"
	case nats.DeliverLastPolicy:
		return "only the last message"
	case nats.DeliverNewPolicy:
		return "new messages only"
	case nats.DeliverByStartSequencePolicy:
		return "from a sequence number"
	case nats.DeliverByStartTimePolicy:
		return "from a time"
	case nats.DeliverLastPerSubjectPolicy:
		return "the last message per subject"
	default:
		return fmt.Sprintf("policy %d", int(p))
	}
}

// statusLines builds the block above the map.
func (r *reader) statusLines(now time.Time) []string {
	// The window is always the demo's own running time, never the time since the
	// first stroke. A demo that has been watching a quiet region for ten minutes and
	// then receives one stroke would otherwise report "0s elapsed" and a rate of
	// several hundred a minute, both of which are true of nothing.
	elapsed := now.Sub(r.startedAt)

	// A stroke that arrived a moment ago reads as "live" rather than as an age of
	// zero seconds. During an active storm that is the normal state, and "last 0s"
	// is a strange thing to read several times a second.
	last := "none yet"
	if !r.lastAt.IsZero() {
		if d := now.Sub(r.lastAt); d < 2*time.Second {
			last = "live"
		} else {
			last = formatAge(d) + " ago"
		}
	}

	head := fmt.Sprintf("%s elapsed · %d strokes · %.1f/min · last %s",
		formatAge(elapsed), r.inside, rateOf(r.inside, elapsed), last)
	if r.haveClosest {
		head += fmt.Sprintf(" · closest %.2f km", r.closestKm)
	}

	lines := []string{head, r.bandSummary()}

	if r.inside == 0 && elapsed > quietHintAfter {
		lines = append(lines,
			"nothing yet, which for a quiet region is normal — checking the bridge would not help")
	}
	if r.filtered > 0 {
		lines = append(lines, fmt.Sprintf("%d arrived in the region's cells but outside the radius",
			r.filtered))
	}
	if r.undecod > 0 {
		lines = append(lines, fmt.Sprintf("%d message(s) could not be read", r.undecod))
	}
	return lines
}

// bandSummary renders the certainty breakdown.
//
// The distribution is worth watching in its own right: a run of boundary strokes
// means the region is being fed mostly by strokes whose true position is
// genuinely unknown, which a count alone would not show.
func (r *reader) bandSummary() string {
	if len(r.byBand) == 0 {
		return "no strokes yet"
	}

	known := []geo.Certainty{geo.CertaintyIn, geo.CertaintyBoundary, geo.CertaintyOut}
	parts := make([]string, 0, len(r.byBand))
	seen := map[geo.Certainty]bool{}

	for _, band := range known {
		if n := r.byBand[band]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", band, n))
			seen[band] = true
		}
	}
	// Anything the bridge reported that this build does not recognise, rather than
	// dropping it: the upstream is free to change, and an unrecognised band should
	// be visible rather than silently absent.
	for band, n := range r.byBand {
		if !seen[band] {
			parts = append(parts, fmt.Sprintf("%s %d", band, n))
		}
	}
	return strings.Join(parts, "  ·  ")
}

// repaint overwrites the block this demo last drew.
//
// The cursor moves up by exactly the number of lines the previous frame occupied
// and each line is cleared before it is rewritten. Anything else — a fixed count,
// or a clear-screen — either leaves stale rows behind when the frame shrinks, or
// takes the reader's scrollback with it.
func (r *reader) repaint(lines []string) {
	var b strings.Builder
	if r.printed > 0 {
		fmt.Fprintf(&b, "\x1b[%dA", r.printed)
	}
	for _, line := range lines {
		b.WriteString("\x1b[2K")
		b.WriteString(line)
		b.WriteString("\n")
	}
	fmt.Fprint(r.out, b.String())
	r.printed = len(lines)
}

// summary prints what was seen.
//
// It runs on every exit, which makes it the artifact a reader ends up holding when
// they pipe the demo into a file.
func (r *reader) summary() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Anything received since the last frame is flushed first, so that stopping on
	// --limit or on a signal cannot swallow the stroke that stopped it. Log mode
	// batches lines into the frame tick, and the exit is exactly the moment when
	// the last batch is most likely to still be queued.
	if len(r.pending) > 0 {
		for _, line := range r.pending {
			fmt.Fprintln(r.out, line)
		}
		r.pending = nil
		r.printed = 0
	}

	const rule = "------------------------------------------------------------"

	fmt.Fprintf(r.out, "\n%s\n", rule)
	if r.inside == 0 {
		fmt.Fprintf(r.out, "no strokes seen")
		if r.filtered > 0 {
			fmt.Fprintf(r.out, "; %d arrived outside the radius", r.filtered)
		}
		fmt.Fprintln(r.out)
		fmt.Fprint(r.out,
			"A quiet region produces nothing for hours, so this is not by itself evidence of a\n"+
				"fault. To tell the two apart, check that the bridge is running and that its region\n"+
				"matches this one:\n"+
				"  docker compose logs lightningfeed | grep 'configured region'\n"+
				"  lightningfeed cells --region-lat=... --region-lon=... --region-radius-km=...\n")
	} else {
		peak := 0
		for _, n := range r.buckets {
			if n > peak {
				peak = n
			}
		}

		// The window is from the demo's own start, not from the first stroke. A burst
		// delivered the instant the subscription attaches would otherwise report a
		// window of a few milliseconds, and a rate computed over that is a number with
		// no meaning in it.
		window := r.lastAt.Sub(r.startedAt)
		seen := fmt.Sprintf("%d strokes", r.inside)
		switch {
		case window < time.Second:
			// Common when --replay drains a stream that already had strokes in it.
			// A rate here would be an artefact of delivery speed, not of weather.
			seen += " in under a second"
		default:
			seen += fmt.Sprintf(" over %s (%.1f/min)", formatAge(window), rateOf(r.inside, window))
		}

		fmt.Fprintf(r.out, "%s, peak %d per %s\n", seen, peak, bucketEvery)
		fmt.Fprintf(r.out, "certainty:  %s\n", r.bandSummary())
		fmt.Fprintf(r.out, "networks:   %s\n", joinCounts(r.byNet))
		if r.haveClosest {
			fmt.Fprintf(r.out, "closest:    %.3f km from the region centre\n", r.closestKm)
		}
		if r.filtered > 0 {
			fmt.Fprintf(r.out, "ignored:    %d published outside the radius\n", r.filtered)
		}
		if r.undecod > 0 {
			fmt.Fprintf(r.out, "unread:     %d message(s)\n", r.undecod)
		}
	}

	fmt.Fprintf(r.out, "%s\n", feed.Attribution)
	fmt.Fprint(r.out,
		"\nNon-commercial use only, and not for storm warning: the upstream's terms prohibit it,\n"+
			"and a stroke's location carries kilometres of uncertainty.\n")
}

func rateOf(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Minutes()
}

func joinCounts(counts map[string]int) string {
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s %d", k, counts[k]))
	}
	return strings.Join(parts, "  ")
}

// isTerminal reports whether a file is a character device rather than a file or a
// pipe.
func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// terminalWidth reports how many columns are available.
//
// COLUMNS is used rather than an ioctl because that needs neither a syscall nor
// cgo, and every shell worth using sets it. A wider terminal is allowed to widen
// the map; a narrower one is not, because the map is laid out for its own width
// and clipping it would misreport the region's shape.
func terminalWidth() int {
	if n, err := strconv.Atoi(os.Getenv("COLUMNS")); err == nil && n > mapCols {
		return n
	}
	return mapCols
}

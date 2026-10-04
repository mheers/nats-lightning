// Package ingest wires the pipeline together: upstream to filter to publisher to
// store, under a leadership lease.
//
// It is the only package that knows the order these things happen in. Every other
// package is independently testable because none of them reaches for another
// beyond a small interface.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/heers-it/lightningfeed/internal/config"
	"github.com/heers-it/lightningfeed/internal/dedup"
	"github.com/heers-it/lightningfeed/internal/elect"
	"github.com/heers-it/lightningfeed/internal/feed"
	"github.com/heers-it/lightningfeed/internal/geo"
	"github.com/heers-it/lightningfeed/internal/model"
	"github.com/heers-it/lightningfeed/internal/obs"
	"github.com/heers-it/lightningfeed/internal/store"
	"github.com/heers-it/lightningfeed/internal/upstream"
)

// feedSubjectPrefix mirrors feed.SubjectPrefix for printing subjects without
// importing the whole publisher.
const feedSubjectPrefix = feed.SubjectPrefix

// Runner is a handle to a running pipeline.
//
// It exists so the resolved address of the metrics server can reach the caller.
// A port of 0 asks the kernel to choose one, and the chosen port is not knowable
// from the configuration, so anything that needs to scrape the exposition — a
// test, or an operator running a second instance on an ephemeral port — would
// otherwise have nowhere to connect.
type Runner struct {
	// MetricsAddr is the address the metrics and health server bound to.
	MetricsAddr string

	// Metrics is the metric set, for callers that want to read it directly rather
	// than scrape the exposition.
	Metrics *obs.Metrics
}

// Run executes the bridge until ctx ends.
//
// The order matters and is deliberate:
//
//  1. take the leadership lease, so exactly one process connects upstream
//  2. open the store, which belongs to the leader
//  3. resume from the persisted cursor, so a restart does not replay five minutes
//  4. start serving metrics and health, so the process is observable while it works
//  5. connect upstream and stream
func Run(ctx context.Context, cfg *config.Config) error {
	return RunWith(ctx, cfg, nil)
}

// RunWith is Run with a callback for the moment the pipeline becomes observable.
//
// ready is called once the metrics server is listening and leadership is held,
// which is before the upstream connection is established: connecting takes at
// least the configured handshake timeout, and a caller that could not learn where
// the metrics server ended up would have no way to watch a slow connect happen.
// It is called from the goroutine running the pipeline, so it should not block.
//
// The callback exists because the resolved metrics address is otherwise
// unreachable. A port of 0 asks the kernel to choose one, and the configuration
// never learns which, so a caller cannot scrape the exposition without being told.
func RunWith(ctx context.Context, cfg *config.Config, ready func(*Runner)) error {
	logger := cfg.Logger()

	cells, err := cfg.Cells()
	if err != nil {
		return fmt.Errorf("resolving region cells: %w", err)
	}

	if cfg.RegionConfigured() {
		logger.Info("configured region",
			"name", cfg.RegionName,
			"lat", cfg.Region.Lat, "lon", cfg.Region.Lon,
			"radius_km", cfg.Region.RadiusKm,
			"cells", len(cells),
			"boundary_policy", string(cfg.BoundaryPolicy))
	} else {
		logger.Warn("no region configured: publishing every stroke worldwide",
			"note", "set --region-lat, --region-lon and --region-radius-km to narrow this")
	}

	nc, err := nats.Connect(cfg.NATSURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return fmt.Errorf("connecting to NATS at %s: %w", cfg.NATSURL, err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("JetStream is unavailable at %s: %w", cfg.NATSURL, err)
	}

	// Leadership before anything that writes or connects.
	var lock *elect.Lock
	if cfg.LeaderElect {
		lock, err = elect.NewLock(js, "lightningfeed", elect.Options{
			Key:    cfg.LeaderKey,
			Owner:  hostID(),
			Logger: logger,
		})
		if err != nil {
			return err
		}
		logger.Info("waiting for leadership", "key", cfg.LeaderKey)
		if err := lock.Acquire(ctx); err != nil {
			return fmt.Errorf("acquiring leadership: %w", err)
		}
	}

	// The store belongs to the leader, which the lease now guarantees.
	db, err := store.Open(cfg.SQLitePath)
	if err != nil {
		return fmt.Errorf("opening the stroke store: %w", err)
	}
	defer db.Close()

	if cfg.RegionConfigured() {
		if err := db.SaveRegion(ctx, cfg.RegionName, cfg.Region); err != nil {
			return fmt.Errorf("saving the region definition: %w", err)
		}
	}

	// Resume rather than replay. Without this every restart would re-ingest five
	// minutes of history, and every failover to the other upstream would start
	// blind.
	cursor, err := db.LoadCursor(ctx)
	if err != nil {
		return fmt.Errorf("loading the resume cursor: %w", err)
	}

	// The cursor is only meaningful against the upstream that issued those ids.
	//
	// The two upstream servers run independent id sequences — the same minute
	// showed a last id near 1.48M on one and near 17.8M on the other — so a
	// cursor carried across is not a resume, it is a claim about a sequence that
	// has nothing to do with this server. The upstream treats `i` as a hint and
	// replays its window regardless, but the identity filter cannot help either:
	// it keys on (src, id), and the ids of a different server name different
	// strokes that happen to collide.
	//
	// Refusing to start is the honest response. Continuing would republish
	// history as live data, which is the one failure this whole pipeline exists
	// to prevent. Starting cold instead is safe — the backfill window drops the
	// replay — so the operator's options are to point back at the original
	// upstream or to accept a cold start deliberately.
	if cursor.Server != "" && cursor.Server != cfg.UpstreamURL {
		logger.Warn("stored cursor belongs to a different upstream; starting cold rather than resuming",
			"cursor_server", cursor.Server,
			"configured_upstream", cfg.UpstreamURL,
			"note", "the two upstreams issue independent id sequences, so the stored ids mean nothing here")
		cursor = store.Cursor{Sources: map[model.Source]int64{}}
	}

	if len(cursor.Sources) > 0 {
		logger.Info("resuming from the stored cursor", "sources", cursor.Sources, "server", cursor.Server)
	} else {
		logger.Info("no stored cursor: starting cold, a replay is expected")
	}

	publisher, err := feed.NewPublisher(feed.Options{
		NATS:           nc,
		Stream:         cfg.Stream,
		Cells:          cells,
		Region:         cfg.Region,
		BoundaryPolicy: cfg.BoundaryPolicy,
	})
	if err != nil {
		return err
	}

	metrics := obs.NewMetrics(cfg.RegionName)
	metrics.SetConnected(false)
	metrics.SetLeader(lock != nil)

	storeHealthy := true

	filter := dedup.New(dedup.Options{
		ReconnectWindow: cfg.BackfillDrop,
		TTL:             cfg.DedupeTTL,
	})

	client := upstream.New(upstream.Options{
		URL:              cfg.UpstreamURL,
		SourceMask:       cfg.SourceMask,
		Viewport:         cfg.Viewport(),
		LastSeen:         cursor.Sources,
		HandshakeTimeout: cfg.HandshakeTimeout,
		IdleTimeout:      cfg.IdleTimeout,
		ReconnectMin:     cfg.ReconnectMin,
		ReconnectMax:     cfg.ReconnectMax,
		Logger:           logger,
		// Open the backfill window on every connection. Without this the first
		// of the four replay-suppression layers never runs at all: the upstream
		// replays about five minutes of history on each connect, and those
		// strokes are indistinguishable from live ones by identity alone on a
		// cold start, where the filter has never seen them.
		OnSessionStart: filter.StartConnection,
	})

	// Register the gauges that close over the store and the client. They cannot
	// be built in NewMetrics, and a collector that is built but not registered
	// exports nothing at all.
	metrics.Register(
		metrics.LastMessageAgeGauge(client.LastMessage, client.Connected),
		obs.StoreRowsGauge(func() (int64, error) { return db.Count(context.Background(), time.Time{}) }),
	)
	metrics.SetRegionInfo(cfg.RegionName)

	server, err := obs.StartServer(ctx, cfg.MetricsAddr, metrics.Handler(), obs.Probe{
		Connected:    client.Connected,
		LastMessage:  client.LastMessage,
		IsLeader:     func() bool { return lock == nil || lock.IsLeader() },
		StoreHealthy: func() bool { return storeHealthy },
		Version:      "dev",
		Region:       cfg.RegionName,
		ID:           hostID(),
	})
	if err != nil {
		return fmt.Errorf("starting the metrics server: %w", err)
	}
	defer server.Close()

	// Log the address the server actually bound to, not the one requested: with
	// a port of 0 the two differ, and the resolved one is the only one anyone can
	// connect to.
	logger.Info("serving metrics and health", "addr", server.Addr())

	// From here the process is observable, which is the point of starting the
	// server before the upstream connection rather than after.
	if ready != nil {
		ready(&Runner{MetricsAddr: server.Addr(), Metrics: metrics})
	}

	// Renew the lease alongside the stream. Losing it means another process is
	// now the writer, and continuing would mean two writers on one database and
	// two upstream connections.
	leaderCtx, stopLeader := context.WithCancel(ctx)
	defer stopLeader()
	if lock != nil {
		go func() {
			if err := lock.Run(leaderCtx); err != nil && leaderCtx.Err() == nil {
				logger.Error("leadership lost, shutting down", "error", err)
				// Cancelling the outer context is the only way to stop Run, and
				// stopping is the correct response to losing the lease.
				stopLeader()
			}
		}()
	}

	// Report connection state and upstream counters into the metrics. The
	// client's counters are monotonic totals and the gauges are point-in-time
	// state, so neither can be wired once at startup: the connected gauge sat at
	// zero for the life of the process, and the reject and reconnect counters
	// were never incremented at all.
	go reportUpstream(leaderCtx, client, lock, metrics)

	// Persist the resume cursor on a timer. Without this the cursor is only ever
	// read, and every restart would replay five minutes of history - which is
	// precisely what loading it at startup was meant to avoid.
	cursorDone := startCursorSaver(leaderCtx, db, client, cfg.UpstreamURL, cfg, logger)

	pruneDone := startPruner(leaderCtx, db, cfg, logger, metrics)

	streamErr := client.Run(leaderCtx, func(ctx context.Context, s model.Stroke) error {
		return process(ctx, s, filter, publisher, db, cfg, metrics, logger)
	})
	stopLeader()
	<-pruneDone
	<-cursorDone

	if errors.Is(streamErr, elect.ErrNotLeader) {
		return streamErr
	}
	if streamErr != nil && !errors.Is(streamErr, context.Canceled) {
		return fmt.Errorf("streaming: %w", streamErr)
	}
	return nil
}

// upstreamReportEvery is how often upstream state is copied into the metrics.
//
// Short enough that a scrape cannot miss a disconnection, long enough that the
// leadership check, which reads the KV lease, is not a hot loop.
const upstreamReportEvery = 2 * time.Second

// reportUpstream mirrors the client's live state into the metrics until ctx ends.
//
// Two things need this. The connected gauge and the leadership gauge are
// point-in-time state that cannot be set once at startup, and the client's
// counters are monotonic totals while the metrics want increments, so the delta
// since the previous poll is what gets added.
func reportUpstream(
	ctx context.Context,
	client *upstream.Client,
	lock *elect.Lock,
	metrics *obs.Metrics,
) {
	ticker := time.NewTicker(upstreamReportEvery)
	defer ticker.Stop()

	var prev struct{ malformed, rejected, reconnects int64 }

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		metrics.SetConnected(client.Connected())
		metrics.SetLeader(lock == nil || lock.IsLeader())

		if n := client.MalformedFrames(); n > prev.malformed {
			metrics.ObserveMalformedFrames(n - prev.malformed)
			prev.malformed = n
		}
		if n := client.RejectedStrokes(); n > prev.rejected {
			metrics.ObserveRejectedStrokes(n - prev.rejected)
			prev.rejected = n
		}
		if n := client.Reconnects(); n > prev.reconnects {
			metrics.ObserveReconnect("upstream", "session_ended", n-prev.reconnects)
			prev.reconnects = n
		}
	}
}

// process applies every stage to one stroke and publishes it.
//
// The stages are ordered from cheapest to most expensive, and each can stop the
// stroke reaching the next.
func process(
	ctx context.Context,
	s model.Stroke,
	filter *dedup.Filter,
	publisher *feed.Publisher,
	db *store.Store,
	cfg *config.Config,
	metrics *obs.Metrics,
	logger *slog.Logger,
) error {
	metrics.StrokesTotal.WithLabelValues(fmt.Sprintf("%d", int(s.Src))).Inc()

	// Stage 1 and 2: replay suppression. Backfill drop, then identity.
	if !filter.Accept(s, time.Now()) {
		metrics.DroppedTotal.WithLabelValues("replay").Inc()
		return nil
	}

	// Stage 3: the region, when one is configured.
	var (
		cell      string
		certainty geo.Certainty
		inside    = true
	)
	if cfg.RegionConfigured() {
		// Assign rather than declare: `cell, err :=` would shadow the outer cell
		// and silently archive every stroke with an empty cell, which breaks the
		// spatial index without producing a single error. Everything would still
		// publish to NATS correctly, because the publisher derives its own cell.
		var err error
		cell, err = geo.Encode(s.Lat, s.Lon, feed.CellPrecision)
		if err != nil {
			metrics.DroppedTotal.WithLabelValues("bad_coordinate").Inc()
			logger.Warn("cannot place stroke", "stroke", s.Key(), "error", err)
			return nil
		}
		if !publisher.AllowsCell(cell) {
			metrics.DroppedTotal.WithLabelValues("outside_region").Inc()
			return nil
		}

		_, band := cfg.Region.ClassifyPoint(s.Lat, s.Lon, deviationOf(s))
		certainty = band
		inside = band == geo.CertaintyIn || (band == geo.CertaintyBoundary && cfg.BoundaryPolicy == geo.PolicyInclude)
		if !inside {
			metrics.DroppedTotal.WithLabelValues("outside_region").Inc()
			return nil
		}
	} else {
		// World-wide: still record a cell, because the archive's spatial index
		// and any later radius query both need one. A failure here is counted
		// rather than swallowed, but it does not drop the stroke: with no region
		// configured there is nothing to be outside of.
		var err error
		cell, err = geo.Encode(s.Lat, s.Lon, feed.CellPrecision)
		if err != nil {
			metrics.DroppedTotal.WithLabelValues("bad_coordinate").Inc()
			logger.Warn("cannot place stroke; publishing without an archive cell", "stroke", s.Key(), "error", err)
		}
	}

	// Stage 4: archive. The store's unique constraint makes a repeat a no-op,
	// so this happens before publishing and cannot fail on a replay.
	if err := db.Record(ctx, s, cell, certainty); err != nil {
		// A store failure must not lose the stroke: carry on and publish, and
		// let the metric show the archive falling behind.
		metrics.DroppedTotal.WithLabelValues("store_error").Inc()
		logger.Error("archiving stroke", "stroke", s.Key(), "error", err)
	}

	// Stage 5: publish, tolerating a brief broker hiccup.
	//
	// Previously a single failed publish propagated out as a sink error, ended
	// the stream, and exited the process. That is the wrong response to a
	// momentary JetStream hiccup: one dropped acknowledgement should not restart
	// the bridge and reconnect to an upstream that throttles connections. A
	// sustained outage still ends the process, because continuing would mean
	// silently discarding every stroke.
	if err := publishWithRetry(ctx, publisher, s, cfg.PublishAttempts, cfg.PublishBackoff); err != nil {
		metrics.PublishedTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("publishing %s: %w", s.Key(), err)
	}
	metrics.PublishedTotal.WithLabelValues("ok").Inc()
	metrics.PublishLagMs.Observe(float64(time.Since(s.Time).Milliseconds()))
	return nil
}

// strokePublisher is the publishing surface process needs.
//
// It is an interface rather than *feed.Publisher so the retry policy around it
// can be exercised against a scripted broker, without a live JetStream for every
// combination of failure and attempt count.
type strokePublisher interface {
	Publish(ctx context.Context, s model.Stroke) error
}

// publishWithRetry publishes a stroke, retrying a bounded number of times.
//
// It retries only on failure, and gives up as soon as the context is done so a
// shutdown is not delayed by a broker that is already gone.
func publishWithRetry(
	ctx context.Context,
	publisher strokePublisher,
	s model.Stroke,
	attempts int,
	backoff time.Duration,
) error {
	if attempts < 1 {
		attempts = 1
	}

	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = publisher.Publish(ctx, s); err == nil {
			return nil
		}
		if attempt == attempts {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return err
}

func deviationOf(s model.Stroke) float64 {
	if s.DeviationM == nil {
		return 0
	}
	return float64(*s.DeviationM)
}

// startPruner deletes old strokes on a timer.
func startPruner(
	ctx context.Context,
	db *store.Store,
	cfg *config.Config,
	logger *slog.Logger,
	metrics *obs.Metrics,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(cfg.PruneEvery)
		defer ticker.Stop()

		prune := func() {
			cutoff := time.Now().Add(-cfg.Retention)
			n, err := db.Prune(ctx, cutoff)
			if err != nil {
				logger.Error("pruning strokes", "error", err)
				return
			}
			if n > 0 {
				metrics.StorePruned.Add(float64(n))
				logger.Info("pruned old strokes", "removed", n, "retention", cfg.Retention)
			}
			if _, err := db.PruneCursors(ctx, cutoff); err != nil {
				logger.Warn("pruning cursors", "error", err)
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				prune()
			}
		}
	}()
	return done
}

// cursorSaveEvery is how often the resume cursor is written.
//
// Short enough that an unclean restart replays very little, long enough that
// writing is not measurable against a stream of a few strokes per second.
const cursorSaveEvery = 30 * time.Second

// startCursorSaver persists the upstream resume cursor on a timer.
//
// The cursor is upstream state, so it belongs in the database rather than in a
// broker: NATS knows nothing about the upstream's id sequences. Recording which
// upstream the ids came from matters too, because the two servers issue
// independent sequences and an id from one means nothing to the other.
func startCursorSaver(
	ctx context.Context,
	db *store.Store,
	client *upstream.Client,
	upstreamURL string,
	cfg *config.Config,
	logger *slog.Logger,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)

		ticker := time.NewTicker(cursorSaveEvery)
		defer ticker.Stop()

		// Write once straight away so a restart within the first interval still
		// finds a cursor.
		save := func() {
			c := store.Cursor{Sources: map[model.Source]int64{}, Server: upstreamURL}
			for _, src := range []model.Source{model.SourceBlitzortung, model.SourceLightningMaps} {
				if id := client.LastSeen(src); id > 0 {
					c.Sources[src] = id
				}
			}
			if len(c.Sources) == 0 {
				return // nothing seen yet; do not overwrite a good cursor
			}
			if err := db.SaveCursor(ctx, c); err != nil {
				logger.Error("saving the resume cursor", "error", err)
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				save()
			}
		}
	}()
	return done
}

// hostID identifies this process in the lease and logs.
func hostID() string {
	host, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}

// PrintHistory reads archived strokes and prints them.
//
// It reads SQLite directly rather than NATS: a radius query is not something
// subject filtering can answer, and this is the query the store exists for.
func PrintHistory(ctx context.Context, cfg *config.Config, since string, limit int) error {
	d, err := time.ParseDuration(since)
	if err != nil {
		return fmt.Errorf("parsing --since %q: %w", since, err)
	}

	db, err := store.Open(cfg.SQLitePath)
	if err != nil {
		return fmt.Errorf("opening the stroke store: %w", err)
	}
	defer db.Close()

	region := cfg.Region
	sinceAt := time.Now().Add(-d)

	var strokes []store.StoredStroke
	if region.RadiusKm > 0 {
		strokes, err = db.StrokesNear(ctx, region, sinceAt)
	} else {
		strokes, err = db.StrokesSince(ctx, sinceAt, limit)
	}
	if err != nil {
		return err
	}

	// A limit of zero or less means "no limit", which is both what StrokesSince
	// already assumes and what the CLI passes when --limit is omitted.
	//
	// Truncating unconditionally was the bug: an omitted --limit arrives as 0, so
	// every documented invocation sliced the result down to strokes[len:], which is
	// empty. The command reported "no strokes recorded" against an archive full of
	// them, and a negative value sliced past the end of the slice and panicked.
	if limit > 0 && len(strokes) > limit {
		strokes = strokes[len(strokes)-limit:]
	}

	out := os.Stdout
	fmt.Fprintf(out, "%s\n", time.Now().Format(time.RFC3339))
	if region.RadiusKm > 0 {
		fmt.Fprintf(out, "region %s within %.4g km of %.4f, %.4f; since %s\n\n",
			cfg.RegionName, region.RadiusKm, region.Lat, region.Lon, since)
	} else {
		fmt.Fprintf(out, "world-wide, since %s\n\n", since)
	}

	if len(strokes) == 0 {
		fmt.Fprintln(out, "no strokes recorded")
		return nil
	}

	counts := map[geo.Certainty]int{}
	for _, s := range strokes {
		counts[s.Certainty]++
		fmt.Fprintf(out, "%s  src=%d id=%-9d %8.4f,%8.4f  dev=%-7s delay=%-7s certainty=%s\n",
			s.Time.Format(time.RFC3339), int(s.Src), s.StrokeID, s.Lat, s.Lon,
			formatOptional(s.DeviationM, "m"), formatOptional(s.DelayMS, "ms"), s.Certainty)
	}

	fmt.Fprintf(out, "\n%d strokes", len(strokes))
	if len(counts) > 0 {
		fmt.Fprint(out, "; by certainty:")
		for _, band := range []geo.Certainty{geo.CertaintyIn, geo.CertaintyBoundary, geo.CertaintyOut} {
			if n := counts[band]; n > 0 {
				fmt.Fprintf(out, " %s=%d", band, n)
			}
		}
	}
	fmt.Fprintln(out)
	return nil
}

func formatOptional(p *int, unit string) string {
	if p == nil {
		return "absent"
	}
	return fmt.Sprintf("%d%s", *p, unit)
}

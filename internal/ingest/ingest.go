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
	metrics.UpstreamConnected.Set(0)
	if lock != nil {
		metrics.LeadershipHeld.Set(1)
	}

	storeHealthy := true
	metrics.StoreRows = obs.GaugeFunc(func() float64 {
		n, err := db.Count(context.Background(), time.Time{})
		if err != nil {
			return 0
		}
		return float64(n)
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
	})

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
	logger.Info("serving metrics and health", "addr", server.Addr())

	filter := dedup.New(dedup.Options{
		ReconnectWindow: cfg.BackfillDrop,
		TTL:             cfg.DedupeTTL,
	})

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
		if !geo.InCells(publisherCells(publisher), cell) {
			metrics.DroppedTotal.WithLabelValues("outside_region").Inc()
			return nil
		}

		dist, band := cfg.Region.ClassifyPoint(s.Lat, s.Lon, deviationOf(s))
		_ = dist
		certainty = band
		inside = band == geo.CertaintyIn || (band == geo.CertaintyBoundary && cfg.BoundaryPolicy == geo.PolicyInclude)
		if !inside {
			metrics.DroppedTotal.WithLabelValues("outside_region").Inc()
			return nil
		}
	} else {
		cell, _ = geo.Encode(s.Lat, s.Lon, feed.CellPrecision)
	}

	// Stage 4: archive. The store's unique constraint makes a repeat a no-op,
	// so this happens before publishing and cannot fail on a replay.
	if err := db.Record(ctx, s, cell, certainty); err != nil {
		// A store failure must not lose the stroke: carry on and publish, and
		// let the metric show the archive falling behind.
		metrics.DroppedTotal.WithLabelValues("store_error").Inc()
		logger.Error("archiving stroke", "stroke", s.Key(), "error", err)
	}

	// Stage 5: publish.
	if err := publisher.Publish(ctx, s); err != nil {
		metrics.PublishedTotal.WithLabelValues("error").Inc()
		return fmt.Errorf("publishing %s: %w", s.Key(), err)
	}
	metrics.PublishedTotal.WithLabelValues("ok").Inc()
	metrics.PublishLagMs.Observe(float64(time.Since(s.Time).Milliseconds()))
	return nil
}

func deviationOf(s model.Stroke) float64 {
	if s.DeviationM == nil {
		return 0
	}
	return float64(*s.DeviationM)
}

// publisherCells exposes the publisher's cell restriction.
func publisherCells(p *feed.Publisher) map[string]struct{} {
	return feed.CellSet(p.Cells())
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

	if len(strokes) > limit {
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

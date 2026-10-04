// Command lightningfeed bridges the lightningmaps.org real-time feed into NATS.
//
// It is a private, non-commercial deployment. Lightning data is
// (c) Blitzortung.org contributors, licensed CC BY-SA 4.0; the upstream's terms
// require that consumers read from a separate server rather than connecting to
// Blitzortung themselves, which is exactly what this bridge is.
package main

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/heers-it/lightningfeed/internal/config"
	"github.com/heers-it/lightningfeed/internal/feed"
	"github.com/heers-it/lightningfeed/internal/ingest"
	"github.com/heers-it/lightningfeed/internal/model"
	"github.com/heers-it/lightningfeed/internal/upstream"
)

// version is stamped at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := newRoot().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "lightningfeed: %v\n", err)
		os.Exit(1)
	}
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "lightningfeed",
		Short: "Bridge real-time lightning strokes onto NATS",
		Long: strings.TrimSpace(`
Bridges the lightningmaps.org real-time stroke feed into a NATS JetStream
stream, and archives it in SQLite for radius queries.

Lightning data (c) Blitzortung.org contributors, CC BY-SA 4.0. Non-commercial
use only. The upstream's terms require consumers to read from a separate server
rather than connecting to Blitzortung directly; this process holds that single
connection on your behalf.`),
		SilenceUsage: true,
		Version:      buildVersion(),
	}

	root.AddCommand(newIngestCmd())
	root.AddCommand(newHistoryCmd())
	root.AddCommand(newCellsCmd())
	return root
}

// buildVersion reports the build, including the module version when stamped at
// release time.
func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok || info.Main.Version == "" {
		return version
	}
	if version != "dev" {
		return version
	}
	return info.Main.Version
}

func newIngestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest",
		Short: "Run the bridge: upstream to NATS and SQLite",
		Long: strings.TrimSpace(`
Connects to the upstream, suppresses replays, filters to the configured
region, publishes to NATS and archives to SQLite.

Only the elected leader connects to the upstream. The upstream throttles new
connections severely, so a second concurrent connection is actively harmful; the
leader lease guarantees exactly one.`),
		// The flags are defined by internal/config, not by cobra, so cobra is
		// told to pass arguments through untouched.
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.ParseFromEnv(args)
			if err != nil {
				return err
			}
			return ingest.Run(cmd.Context(), cfg)
		},
	}
	return cmd
}

func newHistoryCmd() *cobra.Command {
	var (
		since string
		limit int
	)

	cmd := &cobra.Command{
		Use:   "history",
		Short: "Query archived strokes near the configured region",
		Long: strings.TrimSpace(`
Reads strokes from SQLite rather than NATS, because a radius query is not
something subject filtering can answer.

Reads the archive directly and does not disturb a running ingest. The database
file belongs to the leader process; opening it read-only is safe.`),
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			// These three are history-specific rather than pipeline-wide, so
			// they are stripped before the shared configuration sees them;
			// otherwise it would reject them as unknown flags.
			rest, err := extractHistoryFlags(args, &since, &limit)
			if err != nil {
				return err
			}
			cfg, err := config.ParseFromEnv(rest)
			if err != nil {
				return err
			}
			return ingest.PrintHistory(cmd.Context(), cfg, since, limit)
		},
	}
	return cmd
}

// extractHistoryFlags pulls the history-only flags out of an argument list.
//
// These three are specific to the history command rather than the pipeline, so
// they are stripped before the shared configuration parser sees them; it would
// otherwise reject them as unknown. Both the "--flag value" and "--flag=value"
// spellings are accepted, as the standard flag package accepts both.
func extractHistoryFlags(args []string, since *string, limit *int) ([]string, error) {
	var rest []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, inline, hasInline := strings.Cut(arg, "=")

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

		switch name {
		case "--since", "--limit":
		default:
			rest = append(rest, arg)
			continue
		}

		v, err := value()
		if err != nil {
			return nil, err
		}
		switch name {
		case "--since":
			*since = v
		case "--limit":
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("--limit %q: %w", v, err)
			}
			// Zero means "no limit" and is what an omitted flag resolves to, but a
			// negative number is a sign error rather than a request for everything.
			// Treating it as unlimited would answer a typo with a hundred thousand
			// rows of output, and treating it as zero would answer with none.
			if n < 0 {
				return nil, fmt.Errorf("--limit %d: must not be negative; omit it for no limit", n)
			}
			*limit = n
		}
	}
	return rest, nil
}

func newCellsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cells",
		Short: "List the geohash cells the configured region publishes to",
		Long: strings.TrimSpace(`
Prints the subject cells for the configured region.

Useful when wiring up a consumer: a region subscriber needs exactly these
subjects, and printing them avoids reimplementing the cell enumeration.`),
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := config.ParseFromEnv(args)
			if err != nil {
				return err
			}
			cells, err := cfg.Cells()
			if err != nil {
				return err
			}
			if len(cells) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no region configured; every subject is published")
				return nil
			}

			// Every source the mask selects gets its own subjects, because the
			// subject carries the source code and the two networks issue
			// independent ids. Printing only one of them — as this did, by
			// letting each matching bit overwrite the last — would hand a
			// subscriber working with a multi-network mask an incomplete
			// subscription list.
			sources := selectedSources(cfg.SourceMask)

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "%s: %d cells, radius %.4g km around %.4f,%.4f\n",
				cfg.RegionName, len(cells), cfg.Region.RadiusKm, cfg.Region.Lat, cfg.Region.Lon)

			if len(sources) == 0 {
				fmt.Fprintf(out, "\nno source carries data; the mask selects only networks "+
					"observed to be empty\n")
				return nil
			}
			fmt.Fprintf(out, "sources: %s\n", joinSources(sources))

			for _, src := range sources {
				fmt.Fprintf(out, "\n%s\n", model.Source(src).String())
				for _, c := range cells {
					fmt.Fprintf(out, "%s.src.%d.cell.%s\n", feed.SubjectPrefix, src, c)
				}
			}
			return nil
		},
	}
}

// selectedSources returns the source codes a mask actually subscribes to.
//
// Bits that resolve to no source are skipped rather than reported, because the
// reserved and testing networks were observed to carry no data at all. Returning
// them would print subjects nothing is ever published to.
func selectedSources(mask upstream.SrcMask) []int {
	var out []int
	for _, bit := range []upstream.SrcMask{
		upstream.MaskReserved, upstream.MaskBlitzortung,
		upstream.MaskLightningMaps, upstream.MaskTesting,
	} {
		if mask&bit == 0 {
			continue
		}
		if code, ok := bit.SrcForMask(); ok {
			out = append(out, int(code))
		}
	}
	return out
}

// joinSources renders source codes for a one-line summary.
func joinSources(sources []int) string {
	parts := make([]string, 0, len(sources))
	for _, s := range sources {
		parts = append(parts, fmt.Sprintf("src.%d", s))
	}
	return strings.Join(parts, ", ")
}

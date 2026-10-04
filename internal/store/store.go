// Package store persists strokes and the upstream resume cursor in SQLite.
//
// SQLite is here for two things NATS cannot do.
//
// The first is the upstream resume cursor. That is upstream state, so no broker
// can hold it, and losing it across a restart means replaying five minutes of
// history every time. Which of the two upstream servers it belongs to has to be
// recorded alongside it, because the two issue independent id sequences.
//
// The second is the radius query: "every stroke within 10 km of the village
// since yesterday". NATS subject filtering cannot express a radius, and a
// consumer would otherwise have to receive the surrounding cells' worth of
// volume as a firehose and filter it itself.
//
// The driver is modernc.org/sqlite, which is pure Go. That matters for
// deployment: the binary stays statically linked and runs on a box with no C
// toolchain.
//
// The database file belongs to the leader process. SQLite's locking model is
// not built for network storage, so it must not be placed on shared or remote
// filesystems.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver, registers "sqlite"

	"github.com/heers-it/lightningfeed/internal/geo"
	"github.com/heers-it/lightningfeed/internal/model"
)

// schemaVersion is bumped whenever migrations change.
const schemaVersion = "1"

// Store is the persistent store.
type Store struct {
	db *sql.DB
}

// Open opens or creates the database at path and applies the schema.
func Open(path string) (*Store, error) {
	// Busy timeout so a brief lock contention waits rather than failing.
	// WAL plus a single writer means contention should be rare, but a
	// concurrent read during a checkpoint is normal.
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"+
		"&_pragma=foreign_keys(ON)", path)

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: opening %s: %w", path, err)
	}

	// SQLite tolerates one writer. Keeping the pool small avoids pointless
	// lock contention under the leader's single-writer model.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(time.Hour)

	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the database.
func (s *Store) Close() error { return s.db.Close() }

// migrate creates the schema.
//
// Every statement is idempotent so the function can run on every open. There is
// no migration framework: the schema is small and this keeps the deployment to a
// single file with no moving parts.
func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS upstream_cursor (
    src          INTEGER PRIMARY KEY,
    last_id      INTEGER NOT NULL,
    last_time_ms INTEGER NOT NULL,
    server       TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS stroke (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    src         INTEGER NOT NULL,
    stroke_id   INTEGER NOT NULL,
    time_ms     INTEGER NOT NULL,
    received_ms INTEGER NOT NULL,
    lat         REAL    NOT NULL,
    lon         REAL    NOT NULL,
    deviation_m INTEGER,
    delay_ms    INTEGER,
    cell        TEXT    NOT NULL,
    certainty   TEXT    NOT NULL,
    UNIQUE (src, stroke_id)
);

CREATE INDEX IF NOT EXISTS stroke_time_idx ON stroke (time_ms);
CREATE INDEX IF NOT EXISTS stroke_cell_idx ON stroke (cell, time_ms);
CREATE INDEX IF NOT EXISTS stroke_certainty_idx ON stroke (certainty, time_ms);

CREATE TABLE IF NOT EXISTS region (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    name      TEXT NOT NULL,
    lat       REAL NOT NULL,
    lon       REAL NOT NULL,
    radius_km REAL NOT NULL,
    cells     TEXT NOT NULL
);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("store: applying schema: %w", err)
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO meta (k, v) VALUES ('schema_version', ?)`, schemaVersion); err != nil {
		return fmt.Errorf("store: recording schema version: %w", err)
	}
	return nil
}

// StoredStroke is a stroke as persisted, with the judgements that were made
// about it at ingest time.
type StoredStroke struct {
	model.Stroke

	// ReceivedAt is when the bridge first published this stroke, distinct from
	// when the discharge happened.
	ReceivedAt time.Time

	// Cell is the geohash-5 subject cell.
	Cell string

	// Certainty is the band the stroke fell into relative to the configured
	// region, or empty when no region was configured.
	Certainty geo.Certainty
}

// Record persists a stroke.
//
// It is a no-op when the stroke has already been stored, which makes the
// upstream's reconnect replay harmless. That is the fourth and last
// deduplication layer, and the only one whose memory survives a restart.
//
// The certainty band is stored rather than recomputed, so the region definition
// can be widened later without silently reinterpreting history.
func (s *Store) Record(ctx context.Context, stroke model.Stroke, cell string, certainty geo.Certainty) error {
	// An empty cell would be stored happily and then match no radius query, so
	// the archive would look populated while being spatially unsearchable. This
	// happened once, through a variable shadowing bug at the call site, and it
	// produced no error anywhere.
	if cell == "" {
		return fmt.Errorf("store: stroke %s has no geohash cell", stroke.Key())
	}

	const q = `
INSERT OR IGNORE INTO stroke
    (src, stroke_id, time_ms, received_ms, lat, lon, deviation_m, delay_ms, cell, certainty)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	var deviation, delay sql.NullInt64
	if stroke.DeviationM != nil {
		deviation = sql.NullInt64{Int64: int64(*stroke.DeviationM), Valid: true}
	}
	if stroke.DelayMS != nil {
		delay = sql.NullInt64{Int64: int64(*stroke.DelayMS), Valid: true}
	}

	_, err := s.db.ExecContext(ctx, q,
		int(stroke.Src),
		stroke.StrokeID,
		stroke.Time.UnixMilli(),
		time.Now().UnixMilli(),
		stroke.Lat,
		stroke.Lon,
		deviation,
		delay,
		cell,
		string(certainty),
	)
	if err != nil {
		return fmt.Errorf("store: recording stroke %s: %w", stroke.Key(), err)
	}
	return nil
}

// Count returns how many strokes were recorded at or after since. A zero time
// counts everything.
func (s *Store) Count(ctx context.Context, since time.Time) (int64, error) {
	q := `SELECT COUNT(*) FROM stroke`
	var args []any
	if !since.IsZero() {
		q += ` WHERE time_ms >= ?`
		args = append(args, since.UnixMilli())
	}

	var n int64
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("store: counting strokes: %w", err)
	}
	return n, nil
}

// maxRows bounds any history read, so a consumer cannot ask for everything ever
// recorded and exhaust memory.
//
// It bounds rows read from SQLite, not rows returned. Applying it after the exact
// distance test would cap the answer at a number that has nothing to do with the
// question, and a caller would have no way to tell a truncated result from a quiet
// hour.
const maxRows = 10000

// StrokesNear returns strokes within the circle since the given time.
//
// This is the query the whole store exists for: a subject-filtered subscription
// cannot answer it, because a radius is not a subject.
//
// Rows are read up to maxRows and then filtered by exact distance, so a cell that
// only clips the circle still consumes part of that budget. The result is complete
// with respect to the rows read, and at the observed rates for a region of this
// size the cap is never reached.
func (s *Store) StrokesNear(ctx context.Context, c geo.Circle, since time.Time) ([]StoredStroke, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	// Select by the geohash cells covering the circle, then filter exactly by
	// distance. The cell index makes the selection cheap; the distance test is
	// what makes the answer correct, because a cell is a box and the region is a
	// circle.
	cells, err := c.Cells(geo5Precision)
	if err != nil {
		return nil, fmt.Errorf("store: enumerating cells: %w", err)
	}

	// The cell list is passed as one JSON array parameter rather than one
	// placeholder per cell.
	//
	// SQLite caps the number of bound variables per statement, and a large
	// region's cell list exceeds that easily: a 2500 km radius needs millions
	// of geohash-5 cells. Selecting through json_each keeps the statement to two
	// parameters whatever the region size, and the index on (cell, time_ms) is
	// still used.
	encoded, err := json.Marshal(cells)
	if err != nil {
		return nil, fmt.Errorf("store: encoding region cells: %w", err)
	}

	// The limit bounds how many rows are read, not how many are returned. The
	// exact distance test runs afterwards, and a cell only intersects the circle
	// loosely, so a LIMIT applied after that test would silently return fewer
	// strokes than were asked for while looking like a complete answer.
	const q = `
SELECT src, stroke_id, time_ms, received_ms, lat, lon, deviation_m, delay_ms, cell, certainty
  FROM stroke
 WHERE time_ms >= ?
   AND cell IN (SELECT value FROM json_each(?))
 ORDER BY time_ms
 LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, since.UnixMilli(), string(encoded), maxRows)
	if err != nil {
		return nil, fmt.Errorf("store: querying near %v: %w", c, err)
	}
	defer rows.Close()

	var out []StoredStroke
	for rows.Next() {
		st, err := scanStroke(rows)
		if err != nil {
			return nil, err
		}
		if !c.ContainsPoint(st.Lat, st.Lon) {
			continue // in a covering cell but outside the circle
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: reading rows: %w", err)
	}
	return out, nil
}

// StrokesSince returns the most recent strokes, newest last. It is the
// world-wide read, used when no region is configured.
func (s *Store) StrokesSince(ctx context.Context, since time.Time, limit int) ([]StoredStroke, error) {
	if limit <= 0 || limit > maxRows {
		limit = maxRows
	}

	// The newest rows are selected first, because a LIMIT applied to an ascending
	// scan keeps the *oldest* matching rows. Asking for the last ten strokes since
	// yesterday returned the first ten of that window instead, which looks like a
	// working query right up until someone notices the timestamps are hours stale.
	// The result is reversed afterwards to restore the documented newest-last order.
	const q = `
SELECT src, stroke_id, time_ms, received_ms, lat, lon, deviation_m, delay_ms, cell, certainty
  FROM stroke
 WHERE time_ms >= ?
 ORDER BY time_ms DESC
 LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, since.UnixMilli(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: querying since %v: %w", since, err)
	}
	defer rows.Close()

	var out []StoredStroke
	for rows.Next() {
		st, err := scanStroke(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: reading rows: %w", err)
	}

	// Restore the documented newest-last order, which the DESC scan inverted.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// rowScanner is satisfied by both *sql.Row and *sql.Rows.
type rowScanner interface{ Scan(dest ...any) error }

func scanStroke(sc rowScanner) (StoredStroke, error) {
	var (
		st            StoredStroke
		src           int
		timeMs        int64
		receivedMs    int64
		deviation     sql.NullInt64
		delay         sql.NullInt64
		certaintyText string
	)
	err := sc.Scan(
		&src, &st.StrokeID, &timeMs, &receivedMs,
		&st.Lat, &st.Lon, &deviation, &delay, &st.Cell, &certaintyText,
	)
	if err != nil {
		return StoredStroke{}, fmt.Errorf("store: scanning stroke: %w", err)
	}

	st.Src = model.Source(src)
	st.Time = time.UnixMilli(timeMs).UTC()
	st.ReceivedAt = time.UnixMilli(receivedMs).UTC()
	st.Certainty = geo.Certainty(certaintyText)
	if deviation.Valid {
		v := int(deviation.Int64)
		st.DeviationM = &v
	}
	if delay.Valid {
		v := int(delay.Int64)
		st.DelayMS = &v
	}
	return st, nil
}

// geo5Precision matches the subject granularity so the stored cell and the
// published subject always agree.
const geo5Precision = 5

// Cursor is the upstream resume state.
//
// Server is recorded because the two upstream servers issue independent id
// sequences: an id from one means nothing to the other.
type Cursor struct {
	// Sources maps a network to the highest stroke id processed.
	Sources map[model.Source]int64

	// Server is which upstream the ids came from, "live" or "live2".
	Server string
}

// SaveCursor persists the resume state.
func (s *Store) SaveCursor(ctx context.Context, c Cursor) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: beginning cursor transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM upstream_cursor`); err != nil {
		return fmt.Errorf("store: clearing cursor: %w", err)
	}

	nowMs := time.Now().UnixMilli()
	for src, lastID := range c.Sources {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO upstream_cursor (src, last_id, last_time_ms, server) VALUES (?, ?, ?, ?)`,
			int(src), lastID, nowMs, c.Server); err != nil {
			return fmt.Errorf("store: saving cursor for src %d: %w", int(src), err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: committing cursor: %w", err)
	}
	return nil
}

// LoadCursor reads the resume state. A fresh database yields an empty cursor
// rather than an error.
func (s *Store) LoadCursor(ctx context.Context) (Cursor, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT src, last_id, server FROM upstream_cursor`)
	if err != nil {
		return Cursor{}, fmt.Errorf("store: loading cursor: %w", err)
	}
	defer rows.Close()

	out := Cursor{Sources: map[model.Source]int64{}}
	for rows.Next() {
		var src, lastID int64
		var server string
		if err := rows.Scan(&src, &lastID, &server); err != nil {
			return Cursor{}, fmt.Errorf("store: scanning cursor: %w", err)
		}
		out.Sources[model.Source(src)] = lastID
		if out.Server == "" {
			out.Server = server
		}
	}
	if err := rows.Err(); err != nil {
		return Cursor{}, fmt.Errorf("store: reading cursor: %w", err)
	}
	return out, nil
}

// Prune deletes strokes older than the cutoff and returns how many went.
func (s *Store) Prune(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM stroke WHERE time_ms < ?`, cutoff.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("store: pruning: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: counting pruned rows: %w", err)
	}
	return n, nil
}

// PruneCursors deletes resume entries older than the cutoff. A cursor for a
// network that has been quiet for weeks is not worth keeping.
func (s *Store) PruneCursors(ctx context.Context, cutoff time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM upstream_cursor WHERE last_time_ms < ?`, cutoff.UnixMilli())
	if err != nil {
		return 0, fmt.Errorf("store: pruning cursors: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// SaveRegion stores the region definition.
//
// Nothing in the process reads this back: the region is configuration, so it comes
// from the flags on every start rather than from the database. It is kept as a
// plain row so an operator can tell after the fact which cells a given archive was
// actually collecting for, which is not otherwise recoverable from the strokes
// themselves. LoadRegion was removed with its readers.
func (s *Store) SaveRegion(ctx context.Context, name string, c geo.Circle) error {
	cells, err := c.Cells(geo5Precision)
	if err != nil {
		return fmt.Errorf("store: enumerating region cells: %w", err)
	}
	encoded, err := json.Marshal(cells)
	if err != nil {
		return fmt.Errorf("store: encoding region cells: %w", err)
	}

	const q = `
INSERT INTO region (id, name, lat, lon, radius_km, cells) VALUES (1, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    name = excluded.name, lat = excluded.lat, lon = excluded.lon,
    radius_km = excluded.radius_km, cells = excluded.cells`

	if _, err := s.db.ExecContext(ctx, q, name, c.Lat, c.Lon, c.RadiusKm, string(encoded)); err != nil {
		return fmt.Errorf("store: saving region: %w", err)
	}
	return nil
}

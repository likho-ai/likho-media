// Package store keeps what is known about each uploaded file in PostgreSQL.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Statuses of a file.
const (
	StatusUploaded = "uploaded" // stored, waiting to be converted
	StatusReady    = "ready"    // it is audio, the waveform exists, a browser can play it
	StatusFailed   = "failed"   // cannot be converted
)

var (
	// ErrNotFound means there is no file with that id.
	ErrNotFound = errors.New("media not found")
	// ErrExists means a file with that id is already stored.
	ErrExists = errors.New("media already exists")
)

// Media is one uploaded file.
type Media struct {
	ID            string
	WorkspaceID   string
	RecordingID   string
	OriginalName  string
	ContentType   string
	SizeBytes     int64
	SHA256        string
	Status        string
	FailureCode   string
	FailureReason string
	OriginalKey   string
	// PlaybackKey is empty when browsers can play the original.
	PlaybackKey         string
	PlaybackContentType string
	PeaksKey            string
	DurationSeconds     float64
	Channels            int
	SampleRate          int
	Codec               string
	Attempts            int
	// UploadedPublished and OutcomePublished say whether the events about this file went out.
	UploadedPublished bool
	OutcomePublished  bool
	CreatedAt         time.Time
}

const columns = `id, workspace_id, recording_id, original_name, content_type, size_bytes, sha256, status,
	failure_code, failure_reason, original_key, playback_key, playback_content_type, peaks_key, duration_seconds,
	channels, sample_rate, codec, attempts, uploaded_published_at IS NOT NULL, outcome_published_at IS NOT NULL, created_at`

func scan(row pgx.Row) (Media, error) {
	var m Media
	err := row.Scan(&m.ID, &m.WorkspaceID, &m.RecordingID, &m.OriginalName, &m.ContentType, &m.SizeBytes,
		&m.SHA256, &m.Status, &m.FailureCode, &m.FailureReason, &m.OriginalKey, &m.PlaybackKey,
		&m.PlaybackContentType, &m.PeaksKey, &m.DurationSeconds, &m.Channels, &m.SampleRate, &m.Codec, &m.Attempts, &m.UploadedPublished, &m.OutcomePublished,
		&m.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Media{}, ErrNotFound
	}
	return m, err
}

// Store is the database.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the database.
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connections.
func (s *Store) Close() { s.pool.Close() }

// Ping reports whether the database answers.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate creates or updates the tables. Safe when several instances start together.
func (s *Store) Migrate(ctx context.Context) error {
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	// One instance migrates at a time; the others wait and then find nothing to do.
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('likho-media-migrate'))`); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext('likho-media-migrate'))`)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	for _, name := range names {
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		sql, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("%s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// CountByStatus says how many media are in each status (for the metrics).
func (s *Store) CountByStatus(ctx context.Context) (map[string]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM media GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{StatusUploaded: 0, StatusReady: 0, StatusFailed: 0}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		counts[status] = n
	}
	return counts, rows.Err()
}

// Insert stores a file that has just arrived. It returns ErrExists when the id is taken.
func (s *Store) Insert(ctx context.Context, m Media) (Media, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO media (id, workspace_id, recording_id, original_name, content_type, size_bytes, sha256, status, original_key)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'uploaded', $8)
		RETURNING `+columns,
		m.ID, m.WorkspaceID, m.RecordingID, m.OriginalName, m.ContentType, m.SizeBytes, m.SHA256, m.OriginalKey)
	stored, err := scan(row)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return Media{}, ErrExists
	}
	return stored, err
}

// Get returns one file.
func (s *Store) Get(ctx context.Context, id string) (Media, error) {
	return scan(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM media WHERE id = $1`, id))
}

// FindBySHA256 returns the oldest usable file with this content in the workspace, other than
// exceptID. It returns ErrNotFound when the content is new.
func (s *Store) FindBySHA256(ctx context.Context, workspaceID, sha256, exceptID string) (Media, error) {
	return scan(s.pool.QueryRow(ctx, `
		SELECT `+columns+` FROM media
		WHERE workspace_id = $1 AND sha256 = $2 AND id <> $3 AND status <> 'failed'
		ORDER BY created_at LIMIT 1`, workspaceID, sha256, exceptID))
}

// Claim hands out the oldest file waiting for conversion, or ErrNotFound when none waits.
// A file held longer than claimTimeout is handed out again: its worker is assumed dead.
func (s *Store) Claim(ctx context.Context, claimTimeout time.Duration) (Media, error) {
	return scan(s.pool.QueryRow(ctx, `
		UPDATE media SET claimed_at = now(), attempts = attempts + 1, updated_at = now()
		WHERE id = (
			SELECT id FROM media
			WHERE status = 'uploaded' AND (claimed_at IS NULL OR claimed_at < now() - make_interval(secs => $1))
			ORDER BY created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+columns, claimTimeout.Seconds()))
}

// Release gives a claimed file back so it is tried again after the delay.
func (s *Store) Release(ctx context.Context, id string, claimTimeout, delay time.Duration) error {
	// A claim that looks (timeout - delay) old runs out after the delay.
	_, err := s.pool.Exec(ctx, `
		UPDATE media SET claimed_at = now() - make_interval(secs => $2), updated_at = now()
		WHERE id = $1 AND status = 'uploaded'`, id, (claimTimeout - delay).Seconds())
	return err
}

// Result is what conversion found out about a file.
type Result struct {
	PlaybackKey         string
	PlaybackContentType string
	PeaksKey            string
	DurationSeconds     float64
	Channels            int
	SampleRate          int
	Codec               string
}

// MarkReady records a finished conversion.
func (s *Store) MarkReady(ctx context.Context, id string, r Result) (Media, error) {
	return scan(s.pool.QueryRow(ctx, `
		UPDATE media SET status = 'ready', playback_key = $2, playback_content_type = $3, peaks_key = $4,
			duration_seconds = $5, channels = $6, sample_rate = $7, codec = $8, claimed_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'uploaded'
		RETURNING `+columns,
		id, r.PlaybackKey, r.PlaybackContentType, r.PeaksKey, r.DurationSeconds, r.Channels, r.SampleRate, r.Codec))
}

// MarkFailed records that the file cannot be converted, and why.
func (s *Store) MarkFailed(ctx context.Context, id, code, reason string) (Media, error) {
	return scan(s.pool.QueryRow(ctx, `
		UPDATE media SET status = 'failed', failure_code = $2, failure_reason = $3, claimed_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'uploaded'
		RETURNING `+columns, id, code, reason))
}

// MarkUploadedPublished records that the "uploaded" event went out.
func (s *Store) MarkUploadedPublished(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE media SET uploaded_published_at = now() WHERE id = $1`, id)
	return err
}

// MarkOutcomePublished records that the "ready" or "failed" event went out.
func (s *Store) MarkOutcomePublished(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `UPDATE media SET outcome_published_at = now() WHERE id = $1`, id)
	return err
}

// Unpublished returns finished files whose "ready" or "failed" event has not gone out,
// which happens when the service stopped between storing the result and publishing it.
func (s *Store) Unpublished(ctx context.Context, olderThan time.Duration, limit int) ([]Media, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+columns+` FROM media
		WHERE status <> 'uploaded' AND outcome_published_at IS NULL AND updated_at < now() - make_interval(secs => $1)
		ORDER BY updated_at LIMIT $2`, olderThan.Seconds(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var found []Media
	for rows.Next() {
		m, err := scan(rows)
		if err != nil {
			return nil, err
		}
		found = append(found, m)
	}
	return found, rows.Err()
}

// Delete removes a file's row and returns what it was, so its objects can be removed too.
func (s *Store) Delete(ctx context.Context, id string) (Media, error) {
	return scan(s.pool.QueryRow(ctx, `DELETE FROM media WHERE id = $1 RETURNING `+columns, id))
}

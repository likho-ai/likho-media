// Package testenv helps tests that need the likho-infra stack (PostgreSQL, NATS, the object
// store) and FFmpeg.
//
// Start the stack first:  likho-infra> bash scripts/up.sh   (or .\stack.ps1 up)
// Without it these tests are skipped locally; with LIKHO_REQUIRE_STACK=1 (set in CI) they fail instead.
package testenv

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/likho-ai/likho-media/internal/config"
	"github.com/likho-ai/likho-media/internal/ids"
)

// Config returns the service's configuration for a test: the local stack, its own database
// schema (dropped when the test ends), free ports, and links that point straight at the instance.
func Config(t *testing.T) config.Config {
	t.Helper()
	t.Setenv("LIKHO_ENV", "test")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	requireStack(t, cfg)

	cfg.HTTPPort, cfg.GRPCPort = 0, 0
	cfg.PublicURL = ""
	cfg.MaxUploadMB = 1
	cfg.WorkDir = t.TempDir()
	cfg.DatabaseURL = schemaURL(t, cfg.DatabaseURL)
	return cfg
}

func requireStack(t *testing.T, cfg config.Config) {
	t.Helper()
	var missing []string
	for name, address := range map[string]string{
		"PostgreSQL": hostOf(cfg.DatabaseURL), "NATS": hostOf(cfg.NATSURL), "the object store": hostOf(cfg.S3Endpoint),
	} {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err != nil {
			missing = append(missing, name)
			continue
		}
		_ = conn.Close()
	}
	for _, tool := range []string{cfg.FFmpegPath, cfg.FFprobePath} {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, tool)
		}
	}
	if len(missing) == 0 {
		return
	}
	message := strings.Join(missing, ", ") + " not available; start the likho-infra stack and install FFmpeg"
	if os.Getenv("LIKHO_REQUIRE_STACK") == "1" {
		t.Fatal(message)
	}
	t.Skip(message)
}

func hostOf(address string) string {
	parsed, err := url.Parse(address)
	if err != nil {
		return address
	}
	return parsed.Host
}

// schemaURL creates an empty schema for this test and returns a database URL that uses it.
func schemaURL(t *testing.T, databaseURL string) string {
	t.Helper()
	schema := "test_" + strings.ToLower(ids.New("sch")[4:])
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatalf("database: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, err := pgx.Connect(ctx, databaseURL)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close(context.Background()) }()
		_, _ = conn.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	})

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// Tone writes a short sine tone to a new file with the given extension (".wav", ".mp3") and
// returns its path. codec is an FFmpeg encoder name ("pcm_mulaw"), or "" for the format's usual one.
// The audio is generated, so no recording is part of this repository.
func Tone(t *testing.T, cfg config.Config, ext, codec string, seconds float64, channels, sampleRate int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tone"+ext)
	args := []string{"-nostdin", "-v", "error", "-y",
		"-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=440:duration=%g", seconds),
		"-ac", fmt.Sprint(channels), "-ar", fmt.Sprint(sampleRate)}
	if codec != "" {
		args = append(args, "-c:a", codec)
	}
	out, err := exec.Command(cfg.FFmpegPath, append(args, path)...).CombinedOutput() //nolint:gosec // fixed arguments
	if err != nil {
		t.Fatalf("ffmpeg could not make a test tone: %v\n%s", err, out)
	}
	return path
}

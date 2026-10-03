// Package config reads the service's settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// DevLinkSecret signs links on a developer's machine. It is refused in production.
const DevLinkSecret = "likho-dev-link-secret" //nolint:gosec // a development default, refused in production

// Config is everything that can be set from outside. The defaults match the likho-infra local stack.
type Config struct {
	Env      string
	LogLevel string

	HTTPPort int // uploads, audio, /healthz, /readyz
	GRPCPort int // likho.media.v1.MediaService

	DatabaseURL string
	NATSURL     string

	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	// One bucket per kind of object.
	BucketOriginal string
	BucketPlayback string
	BucketPeaks    string

	// PublicURL is the address browsers reach this service at (the gateway). Upload links and
	// links to the audio and the peaks start with it.
	PublicURL string
	// InternalURL is the address other services reach this service at; links to the original
	// start with it (only services read originals). Empty = PublicURL. In a cluster, where the
	// public address is not reachable from inside, it is the service's own name and port.
	InternalURL string
	// LinkSecret signs upload and download links.
	LinkSecret  string
	DownloadTTL time.Duration
	UploadTTL   time.Duration
	MaxUploadMB int64

	// Workers is how many files are converted at the same time.
	Workers int
	// MaxAttempts is how often a conversion is tried when a dependency fails, before giving up.
	MaxAttempts int
	// ClaimTimeout is how long a worker may hold a file before another worker may take it.
	ClaimTimeout time.Duration
	FFmpegPath   string
	FFprobePath  string
	// WorkDir holds files while they are received and converted. Empty = the system's temp folder.
	WorkDir string
}

// Load reads the configuration and checks it.
func Load() (Config, error) {
	var problems []string
	number := func(name string, fallback int) int {
		raw, ok := os.LookupEnv(name)
		if !ok || raw == "" {
			return fallback
		}
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			problems = append(problems, fmt.Sprintf("%s must be a whole number, got %q", name, raw))
			return fallback
		}
		return value
	}
	text := func(name, fallback string) string {
		if value, ok := os.LookupEnv(name); ok && value != "" {
			return value
		}
		return fallback
	}

	cfg := Config{
		Env:            text("LIKHO_ENV", "development"),
		LogLevel:       text("LOG_LEVEL", "INFO"),
		HTTPPort:       number("HTTP_PORT", 4010),
		GRPCPort:       number("GRPC_PORT", 5010),
		DatabaseURL:    text("DATABASE_URL", "postgres://likho_media:likho_media@localhost:5433/likho_media"),
		NATSURL:        text("NATS_URL", "nats://localhost:4222"),
		S3Endpoint:     text("S3_ENDPOINT", "http://localhost:9000"),
		S3AccessKey:    text("S3_ACCESS_KEY", "likho-dev"),
		S3SecretKey:    text("S3_SECRET_KEY", "likho-dev-secret"),
		S3Region:       text("S3_REGION", "us-east-1"),
		BucketOriginal: text("S3_BUCKET_ORIGINAL", "likho-audio"),
		BucketPlayback: text("S3_BUCKET_PLAYBACK", "likho-normalized"),
		BucketPeaks:    text("S3_BUCKET_PEAKS", "likho-peaks"),
		PublicURL:      strings.TrimRight(text("PUBLIC_URL", "http://localhost:8080"), "/"),
		InternalURL:    strings.TrimRight(text("INTERNAL_URL", ""), "/"),
		LinkSecret:     text("LINK_SECRET", DevLinkSecret),
		DownloadTTL:    time.Duration(number("DOWNLOAD_TTL_SECONDS", 900)) * time.Second,
		UploadTTL:      time.Duration(number("UPLOAD_TTL_SECONDS", 3600)) * time.Second,
		MaxUploadMB:    int64(number("MAX_UPLOAD_MB", 500)),
		Workers:        number("WORKERS", 2),
		MaxAttempts:    number("MAX_ATTEMPTS", 5),
		ClaimTimeout:   time.Duration(number("CLAIM_TIMEOUT_SECONDS", 600)) * time.Second,
		FFmpegPath:     text("FFMPEG_PATH", "ffmpeg"),
		FFprobePath:    text("FFPROBE_PATH", "ffprobe"),
		WorkDir:        text("WORK_DIR", ""),
	}

	if (cfg.Env == "staging" || cfg.Env == "production") && cfg.LinkSecret == DevLinkSecret {
		problems = append(problems, "LINK_SECRET must be set in "+cfg.Env)
	}
	if cfg.Workers < 1 {
		problems = append(problems, "WORKERS must be at least 1")
	}
	if cfg.MaxAttempts < 1 {
		problems = append(problems, "MAX_ATTEMPTS must be at least 1")
	}
	if cfg.MaxUploadMB < 1 {
		problems = append(problems, "MAX_UPLOAD_MB must be at least 1")
	}
	if len(problems) > 0 {
		return Config{}, errors.New(strings.Join(problems, "; "))
	}
	return cfg, nil
}

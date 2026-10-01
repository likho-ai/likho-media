package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultsMatchTheLocalStack(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPort != 4010 || cfg.GRPCPort != 5010 {
		t.Errorf("ports %d and %d, want 4010 and 5010", cfg.HTTPPort, cfg.GRPCPort)
	}
	if cfg.PublicURL != "http://localhost:8080" {
		t.Errorf("public URL %s, want the gateway", cfg.PublicURL)
	}
	if cfg.DownloadTTL != 15*time.Minute || cfg.UploadTTL != time.Hour {
		t.Errorf("link lifetimes %s and %s", cfg.DownloadTTL, cfg.UploadTTL)
	}
}

func TestEnvironmentVariablesAreRead(t *testing.T) {
	t.Setenv("HTTP_PORT", "9999")
	t.Setenv("PUBLIC_URL", "https://likho.example.com/")
	t.Setenv("WORKERS", "4")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPort != 9999 || cfg.Workers != 4 {
		t.Errorf("got port %d and %d workers", cfg.HTTPPort, cfg.Workers)
	}
	if cfg.PublicURL != "https://likho.example.com" {
		t.Errorf("public URL %q, want it without the trailing slash", cfg.PublicURL)
	}
}

func TestProblemsAreReportedTogether(t *testing.T) {
	t.Setenv("LIKHO_ENV", "production")
	t.Setenv("HTTP_PORT", "http")
	t.Setenv("WORKERS", "0")
	_, err := Load()
	if err == nil {
		t.Fatal("want an error")
	}
	for _, part := range []string{"LINK_SECRET must be set in production", "HTTP_PORT must be a whole number", "WORKERS must be at least 1"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not mention %q", err, part)
		}
	}
}

func TestProductionWithItsOwnSecretIsAccepted(t *testing.T) {
	t.Setenv("LIKHO_ENV", "production")
	t.Setenv("LINK_SECRET", "a-long-random-value")
	if _, err := Load(); err != nil {
		t.Fatal(err)
	}
}

package config

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestEnvFilesOfTheEnvironmentAreReadInOrder(t *testing.T) {
	folder := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(folder, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(".env", "HTTP_PORT=1\nLOG_LEVEL=from-dot-env # a comment\n")
	write(".env.local", "HTTP_PORT=2\n")
	write(".env.staging", "HTTP_PORT=3\nGRPC_PORT='33'\n")
	write(".env.staging.local", "HTTP_PORT=4\nexport WORKERS=\"7\"\nLINK_SECRET=a-staging-secret\n")
	write(".env.production", "HTTP_PORT=5\n")
	t.Chdir(folder)
	t.Setenv("LIKHO_ENV", "staging")
	t.Setenv("GRPC_PORT", "99") // the real environment wins
	for _, name := range []string{"HTTP_PORT", "LOG_LEVEL", "WORKERS"} {
		t.Setenv(name, "")
		_ = os.Unsetenv(name)
	}

	read, err := LoadEnvFiles()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{".env", ".env.local", ".env.staging", ".env.staging.local"}; !slices.Equal(read, want) {
		t.Fatalf("read %v, want %v", read, want)
	}
	got := map[string]string{}
	for _, name := range []string{"HTTP_PORT", "LOG_LEVEL", "GRPC_PORT", "WORKERS"} {
		got[name] = os.Getenv(name)
	}
	want := map[string]string{"HTTP_PORT": "4", "LOG_LEVEL": "from-dot-env", "GRPC_PORT": "99", "WORKERS": "7"}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("%s = %q, want %q", name, got[name], value)
		}
	}

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPPort != 4 || cfg.GRPCPort != 99 || cfg.Workers != 7 {
		t.Fatalf("loaded %+v", cfg)
	}
}

func TestABrokenEnvFileIsReported(t *testing.T) {
	folder := t.TempDir()
	if err := os.WriteFile(filepath.Join(folder, ".env"), []byte("THIS IS NOT A SETTING\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(folder)
	if _, err := LoadEnvFiles(); err == nil {
		t.Fatal("want an error")
	}
}

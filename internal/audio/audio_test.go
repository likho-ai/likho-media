package audio_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/likho-ai/likho-media/internal/audio"
	"github.com/likho-ai/likho-media/internal/testenv"
)

func pcm(samples ...int16) *bytes.Reader {
	var buffer bytes.Buffer
	for _, sample := range samples {
		_ = binary.Write(&buffer, binary.LittleEndian, sample)
	}
	return bytes.NewReader(buffer.Bytes())
}

func TestPeaksAreTheLoudestSampleOfEachSlice(t *testing.T) {
	const slice = audio.SampleRate / audio.PeaksPerSecond // 800 samples = 50 ms
	samples := make([]int16, 2*slice+slice/2)             // two and a half slices
	samples[10] = 16384                                   // first slice: half scale
	samples[slice+5] = -32768                             // second slice: full scale, negative
	samples[2*slice+1] = 3277                             // the half slice at the end: a tenth

	peaks, err := audio.PeaksFromPCM(pcm(samples...))
	if err != nil {
		t.Fatal(err)
	}
	if want := []int{50, 100, 10}; !slices.Equal(peaks.Peaks, want) {
		t.Fatalf("peaks %v, want %v", peaks.Peaks, want)
	}
	if peaks.Max != 100 || peaks.PeaksPerSecond != 20 || peaks.Version != 1 {
		t.Fatalf("unexpected header %+v", peaks)
	}
	if math.Abs(peaks.DurationSeconds-0.125) > 0.0005 {
		t.Fatalf("duration %v, want 0.125", peaks.DurationSeconds)
	}
}

func TestPeaksOfNothing(t *testing.T) {
	peaks, err := audio.PeaksFromPCM(pcm())
	if err != nil {
		t.Fatal(err)
	}
	if peaks.Peaks == nil || len(peaks.Peaks) != 0 || peaks.DurationSeconds != 0 {
		t.Fatalf("want an empty list (not null) and no duration, got %+v", peaks)
	}
}

func TestSilenceHasFlatPeaks(t *testing.T) {
	peaks, err := audio.PeaksFromPCM(pcm(make([]int16, audio.SampleRate)...))
	if err != nil {
		t.Fatal(err)
	}
	if len(peaks.Peaks) != audio.PeaksPerSecond || slices.Max(peaks.Peaks) != 0 {
		t.Fatalf("one second of silence gave %v", peaks.Peaks)
	}
}

func TestProbeAndDecode(t *testing.T) {
	cfg := testenv.Config(t)
	tools := audio.Tools{FFmpeg: cfg.FFmpegPath, FFprobe: cfg.FFprobePath}
	ctx := context.Background()
	if err := tools.Check(ctx); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name, ext, encoder string
		rate               int
		codec, browser     string
	}{
		{"wav", ".wav", "", 44100, "pcm_s16le", "audio/wav"},
		{"mp3", ".mp3", "", 44100, "mp3", "audio/mpeg"},
		{"flac", ".flac", "", 44100, "flac", "audio/flac"},
		// Telephone codecs: FFmpeg reads them, browsers do not.
		{"mu-law wav", ".wav", "pcm_mulaw", 8000, "pcm_mulaw", ""},
		{"a-law wav", ".wav", "pcm_alaw", 8000, "pcm_alaw", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			source := testenv.Tone(t, cfg, c.ext, c.encoder, 2, 2, c.rate)
			info, err := tools.Probe(ctx, source)
			if err != nil {
				t.Fatal(err)
			}
			if info.Channels != 2 || info.SampleRate != c.rate || math.Abs(info.DurationSeconds-2) > 0.1 || info.Codec != c.codec {
				t.Fatalf("probe says %+v, want 2 channels of %s at %d Hz for 2 s", info, c.codec, c.rate)
			}
			if got := info.BrowserContentType(); got != c.browser {
				t.Fatalf("browsers play it as %q, want %q", got, c.browser)
			}

			folder := t.TempDir()
			raw, playback := filepath.Join(folder, "audio.pcm"), ""
			if c.browser == "" {
				playback = filepath.Join(folder, "playback.mp3")
			}
			if err := tools.Decode(ctx, source, raw, playback); err != nil {
				t.Fatal(err)
			}
			if playback != "" {
				copyInfo, err := tools.Probe(ctx, playback)
				if err != nil {
					t.Fatal(err)
				}
				if copyInfo.BrowserContentType() != audio.PlaybackContentType || copyInfo.Channels != 2 ||
					math.Abs(copyInfo.DurationSeconds-2) > 0.2 {
					t.Fatalf("the playback copy is %+v, want an MP3 with both channels, 2 s long", copyInfo)
				}
			}
			peaks, err := audio.PeaksFromFile(raw)
			if err != nil {
				t.Fatal(err)
			}
			if n := len(peaks.Peaks); n < 39 || n > 42 {
				t.Fatalf("%d peaks for 2 s, want about 40", n)
			}
			if loudest := slices.Max(peaks.Peaks); loudest < 5 || loudest > 100 {
				t.Fatalf("a tone should be visible in the waveform, loudest peak is %d", loudest)
			}
		})
	}
}

func TestAFileThatIsNotAudioIsUnreadable(t *testing.T) {
	cfg := testenv.Config(t)
	tools := audio.Tools{FFmpeg: cfg.FFmpegPath, FFprobe: cfg.FFprobePath}
	path := filepath.Join(t.TempDir(), "notes.mp3")
	if err := os.WriteFile(path, []byte("this is not audio, whatever the name says"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tools.Probe(context.Background(), path); !errors.Is(err, audio.ErrUnreadable) {
		t.Fatalf("got %v, want ErrUnreadable", err)
	}
}

func TestAMissingToolIsNotAnUnreadableFile(t *testing.T) {
	tools := audio.Tools{FFmpeg: "no-such-ffmpeg", FFprobe: "no-such-ffprobe"}
	if err := tools.Check(context.Background()); err == nil {
		t.Fatal("want an error for tools that are not installed")
	}
	if _, err := tools.Probe(context.Background(), "x.wav"); err == nil || errors.Is(err, audio.ErrUnreadable) {
		t.Fatalf("got %v; a missing tool must not look like a bad file", err)
	}
}

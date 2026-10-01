// Package audio looks into an uploaded file and makes what the player needs from it: the
// waveform peaks, and a copy a browser can play when the file itself is not one.
//
// The speech model reads the file exactly as it was uploaded, so nothing here changes what
// is transcribed. The work is done by ffprobe and ffmpeg, so every format they read is accepted.
package audio

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	// SampleRate the waveform is measured at.
	SampleRate = 16_000
	// PeaksPerSecond is the waveform's resolution.
	PeaksPerSecond = 20
	// PlaybackContentType of the copy made for browsers.
	PlaybackContentType = "audio/mpeg"
)

// ErrUnreadable means the file is not audio these tools can read. Trying again will not help.
var ErrUnreadable = errors.New("the file is not audio that can be read")

// Info is what a file contains.
type Info struct {
	DurationSeconds float64
	Channels        int
	SampleRate      int
	// Codec and Format as ffprobe names them: "mp3" in "mp3", "pcm_mulaw" in "wav".
	Codec  string
	Format string
}

// BrowserContentType is the content type a browser plays this file as, or "" when browsers
// cannot be relied on to play it (telephone codecs such as GSM, A-law and mu-law, AMR, WMA).
func (i Info) BrowserContentType() string {
	switch {
	case i.Codec == "mp3" && i.Format == "mp3":
		return "audio/mpeg"
	case i.Codec == "flac" && i.Format == "flac":
		return "audio/flac"
	case i.Format == "wav" && (i.Codec == "pcm_s16le" || i.Codec == "pcm_u8" || i.Codec == "pcm_s24le"):
		return "audio/wav"
	case i.Codec == "aac" && strings.Contains(i.Format, "mp4"):
		return "audio/mp4"
	}
	return ""
}

// Tools runs ffprobe and ffmpeg.
type Tools struct {
	FFmpeg  string
	FFprobe string
}

// Check reports whether both tools can be started.
func (t Tools) Check(ctx context.Context) error {
	for _, tool := range []string{t.FFmpeg, t.FFprobe} {
		//nolint:gosec // the tools are named in the configuration, never by a caller
		if err := exec.CommandContext(ctx, tool, "-version").Run(); err != nil {
			return fmt.Errorf("%s cannot be started (is FFmpeg installed?): %w", tool, err)
		}
	}
	return nil
}

func run(ctx context.Context, name string, args ...string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...) //nolint:gosec // see Check
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			// The tool ran and refused the file.
			return nil, fmt.Errorf("%w: %s", ErrUnreadable, lastLine(stderr.String()))
		}
		return nil, fmt.Errorf("%s: %w", name, err) // the tool could not be started
	}
	return stdout.Bytes(), nil
}

func lastLine(text string) string {
	lines := strings.Split(strings.TrimSpace(text), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// Probe reads what the first audio stream of a file is.
func (t Tools) Probe(ctx context.Context, path string) (Info, error) {
	out, err := run(ctx, t.FFprobe, "-v", "error", "-select_streams", "a:0",
		"-show_entries", "stream=codec_name,channels,sample_rate:format=duration,format_name", "-of", "json", path)
	if err != nil {
		return Info{}, err
	}
	var parsed struct {
		Streams []struct {
			Codec      string `json:"codec_name"`
			Channels   int    `json:"channels"`
			SampleRate string `json:"sample_rate"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
			Name     string `json:"format_name"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &parsed); err != nil || len(parsed.Streams) == 0 {
		return Info{}, fmt.Errorf("%w: no audio stream", ErrUnreadable)
	}
	rate, _ := strconv.Atoi(parsed.Streams[0].SampleRate)
	duration, _ := strconv.ParseFloat(parsed.Format.Duration, 64)
	if parsed.Streams[0].Channels < 1 || rate < 1 {
		return Info{}, fmt.Errorf("%w: no audio stream", ErrUnreadable)
	}
	return Info{
		DurationSeconds: duration, Channels: parsed.Streams[0].Channels, SampleRate: rate,
		Codec: parsed.Streams[0].Codec, Format: parsed.Format.Name,
	}, nil
}

// Decode writes the audio of source as raw 16-bit mono samples at SampleRate to pcmPath, for
// the waveform. When playbackPath is not empty it also writes an MP3 copy for browsers, in
// the same pass.
func (t Tools) Decode(ctx context.Context, source, pcmPath, playbackPath string) error {
	args := []string{"-nostdin", "-v", "error", "-y", "-i", source,
		"-map", "0:a:0", "-ac", "1", "-ar", strconv.Itoa(SampleRate), "-f", "s16le", pcmPath}
	if playbackPath != "" {
		// Speech at telephone quality: a variable bit rate of about 50 kbit/s per channel is plenty.
		args = append(args, "-map", "0:a:0", "-c:a", "libmp3lame", "-q:a", "7", "-f", "mp3", playbackPath)
	}
	_, err := run(ctx, t.FFmpeg, args...)
	return err
}

// Peaks is the waveform the player draws.
type Peaks struct {
	Version         int     `json:"version"`
	DurationSeconds float64 `json:"duration_seconds"`
	PeaksPerSecond  int     `json:"peaks_per_second"`
	// Max is the value of a full-scale peak.
	Max int `json:"max"`
	// Peaks holds the loudest sample of each slice of time, from 0 (silence) to Max.
	Peaks []int `json:"peaks"`
}

const peaksMax = 100

// PeaksFromPCM reads raw 16-bit little-endian mono samples at SampleRate and returns the waveform.
func PeaksFromPCM(samples io.Reader) (Peaks, error) {
	const window = SampleRate / PeaksPerSecond
	peaks := Peaks{Version: 1, PeaksPerSecond: PeaksPerSecond, Max: peaksMax, Peaks: []int{}}
	buffer := make([]byte, 2*window)
	total := 0
	for {
		n, err := io.ReadFull(samples, buffer)
		count := n / 2
		if count > 0 {
			loudest := 0
			for i := range count {
				value := int(int16(binary.LittleEndian.Uint16(buffer[2*i:]))) //nolint:gosec // reinterpreting the bits is the point
				if value < 0 {
					value = -value
				}
				loudest = max(loudest, value)
			}
			peaks.Peaks = append(peaks.Peaks, min(peaksMax, int(math.Round(float64(loudest)*peaksMax/32767))))
			total += count
		}
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return Peaks{}, err
		}
	}
	peaks.DurationSeconds = math.Round(float64(total)/SampleRate*1000) / 1000
	return peaks, nil
}

// PeaksFromFile is PeaksFromPCM for a file.
func PeaksFromFile(path string) (Peaks, error) {
	file, err := os.Open(path) //nolint:gosec // a path this service created
	if err != nil {
		return Peaks{}, err
	}
	defer func() { _ = file.Close() }()
	return PeaksFromPCM(file)
}

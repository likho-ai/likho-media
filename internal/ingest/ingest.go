// Package ingest looks at uploaded files: it checks that a file is audio, makes its waveform
// and, when browsers cannot play the file, a copy they can, and tells the rest of Likho what
// happened.
//
// The queue is the media table itself. A worker claims the oldest waiting file, so several
// instances of the service share the work without a coordinator, and a file whose worker
// died is claimed again by another one.
package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/likho-ai/likho-media/internal/audio"
	"github.com/likho-ai/likho-media/internal/config"
	"github.com/likho-ai/likho-media/internal/events"
	"github.com/likho-ai/likho-media/internal/metrics"
	"github.com/likho-ai/likho-media/internal/objects"
	"github.com/likho-ai/likho-media/internal/store"
)

// Failure codes of likho.media.failed.v1, and the words shown to people.
const (
	CodeUnreadable   = "audio_unreadable"
	CodeInternal     = "internal"
	ReasonUnreadable = "The file is not audio that can be read"
	ReasonEmpty      = "The recording is empty"
	ReasonGaveUp     = "The file could not be processed"
)

const perFileTimeout = 15 * time.Minute

// Timing says how patient the processor is. The zero value means the defaults.
type Timing struct {
	// IdlePoll is how often an idle worker looks for files it was not told about
	// (those of an instance that died).
	IdlePoll time.Duration
	// Republish is how often events that could not be sent are tried again.
	Republish time.Duration
	// RetryStep is the wait before the second attempt at a file; each later attempt waits one step longer.
	RetryStep time.Duration
}

func (t Timing) withDefaults() Timing {
	if t.IdlePoll <= 0 {
		t.IdlePoll = 5 * time.Second
	}
	if t.Republish <= 0 {
		t.Republish = 15 * time.Second
	}
	if t.RetryStep <= 0 {
		t.RetryStep = 10 * time.Second
	}
	return t
}

// Publisher sends events. The event bus implements it; tests replace it to make it fail.
type Publisher interface {
	Publish(ctx context.Context, subject string, event events.Event) error
}

// Processor converts files.
type Processor struct {
	cfg     config.Config
	store   *store.Store
	objects *objects.Store
	bus     Publisher
	tools   audio.Tools
	timing  Timing
	log     *slog.Logger
	wake    chan struct{}
	metrics *metrics.Metrics
}

// New returns a processor. Call Run to start it.
func New(cfg config.Config, db *store.Store, objs *objects.Store, bus Publisher, timing Timing, log *slog.Logger) *Processor {
	return &Processor{
		cfg:     cfg,
		store:   db,
		objects: objs,
		bus:     bus,
		tools:   audio.Tools{FFmpeg: cfg.FFmpegPath, FFprobe: cfg.FFprobePath},
		timing:  timing.withDefaults(),
		log:     log,
		wake:    make(chan struct{}, 1),
	}
}

// Wake tells the workers that a file has arrived, so they do not wait for the next poll.
func (p *Processor) Wake() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// WithMetrics counts conversions and the events that go out.
func (p *Processor) WithMetrics(m *metrics.Metrics) *Processor {
	p.metrics = m
	return p
}

func (p *Processor) count(ctx context.Context, outcome string, took time.Duration) {
	if p.metrics == nil {
		return
	}
	p.metrics.Conversions.Add(ctx, 1, metrics.Outcome(outcome))
	if took > 0 {
		p.metrics.ConversionSeconds.Record(ctx, took.Seconds(), metrics.Outcome(outcome))
	}
}

// Run converts files until ctx is cancelled. A file in hand is finished first.
func (p *Processor) Run(ctx context.Context) {
	var workers sync.WaitGroup
	for range p.cfg.Workers {
		workers.Go(func() { p.work(ctx) })
	}
	workers.Go(func() { p.republish(ctx) })
	workers.Wait()
}

func (p *Processor) work(ctx context.Context) {
	for ctx.Err() == nil {
		media, err := p.store.Claim(ctx, p.cfg.ClaimTimeout)
		switch {
		case err == nil:
			// Not cancelled with ctx: a file that was started is finished.
			fileCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), perFileTimeout)
			p.handle(fileCtx, media)
			cancel()
			p.Wake() // there may be more; let an idle worker look too
			continue
		case errors.Is(err, store.ErrNotFound):
		case ctx.Err() != nil:
			return
		default:
			p.log.Error("could not look for waiting files", "error", err)
		}
		select {
		case <-ctx.Done():
		case <-p.wake:
		case <-time.After(p.timing.IdlePoll):
		}
	}
}

func (p *Processor) handle(ctx context.Context, media store.Media) {
	log := p.log.With("media_id", media.ID, "attempt", media.Attempts)
	if !media.UploadedPublished {
		if err := p.bus.Publish(ctx, events.UploadedSubject, events.Uploaded(media)); err != nil {
			p.retryLater(ctx, log, media, err)
			return
		}
		if err := p.store.MarkUploadedPublished(ctx, media.ID); err != nil {
			log.Warn("could not record that the uploaded event went out", "error", err)
		}
	}

	started := time.Now()
	result, err := p.convert(ctx, media)
	switch {
	case err == nil:
		ready, err := p.store.MarkReady(ctx, media.ID, result)
		if err != nil {
			p.retryLater(ctx, log, media, err)
			return
		}
		log.Info("ready", "duration_seconds", ready.DurationSeconds, "channels", ready.Channels,
			"sample_rate", ready.SampleRate, "codec", ready.Codec, "playback_copy", ready.PlaybackKey != "",
			"took_ms", time.Since(started).Milliseconds())
		p.count(ctx, "ready", time.Since(started))
		p.publishOutcome(ctx, ready)
	case errors.Is(err, audio.ErrUnreadable), errors.Is(err, objects.ErrNotFound), errors.Is(err, errEmpty):
		reason := ReasonUnreadable
		if errors.Is(err, errEmpty) {
			reason = ReasonEmpty
		}
		p.count(ctx, "failed", time.Since(started))
		p.fail(ctx, log, media, CodeUnreadable, reason, err)
	case media.Attempts >= p.cfg.MaxAttempts:
		p.count(ctx, "failed", time.Since(started))
		p.fail(ctx, log, media, CodeInternal, ReasonGaveUp, err)
	default:
		p.count(ctx, "retry", 0)
		p.retryLater(ctx, log, media, err)
	}
}

var errEmpty = errors.New("no audio samples")

// convert downloads the original, makes the waveform and (if needed) the playback copy, and
// stores them.
func (p *Processor) convert(ctx context.Context, media store.Media) (store.Result, error) {
	folder, err := os.MkdirTemp(p.cfg.WorkDir, "likho-media-")
	if err != nil {
		return store.Result{}, err
	}
	defer func() { _ = os.RemoveAll(folder) }()

	original := filepath.Join(folder, "original"+filepath.Ext(media.OriginalKey))
	if err := p.objects.GetFile(ctx, p.cfg.BucketOriginal, media.OriginalKey, original); err != nil {
		return store.Result{}, err
	}
	info, err := p.tools.Probe(ctx, original)
	if err != nil {
		return store.Result{}, err
	}
	// Browsers play most files as they are. A copy is made only for the formats they cannot
	// play (telephone codecs), because a copy of every call would double what is stored.
	pcm, playback := filepath.Join(folder, "audio.pcm"), ""
	contentType := info.BrowserContentType()
	if contentType == "" {
		playback, contentType = filepath.Join(folder, "playback.mp3"), audio.PlaybackContentType
	}
	if err := p.tools.Decode(ctx, original, pcm, playback); err != nil {
		return store.Result{}, err
	}
	peaks, err := audio.PeaksFromFile(pcm)
	if err != nil {
		return store.Result{}, err
	}
	if len(peaks.Peaks) == 0 {
		return store.Result{}, errEmpty
	}
	if info.DurationSeconds <= 0 { // some containers do not say; the samples do
		info.DurationSeconds = peaks.DurationSeconds
	}

	base := filepath.ToSlash(filepath.Dir(media.OriginalKey))
	result := store.Result{
		PlaybackContentType: contentType,
		PeaksKey:            base + "/peaks.json",
		DurationSeconds:     info.DurationSeconds,
		Channels:            info.Channels,
		SampleRate:          info.SampleRate,
		Codec:               info.Codec,
	}
	if playback != "" {
		result.PlaybackKey = base + "/playback.mp3"
		if err := p.objects.PutFile(ctx, p.cfg.BucketPlayback, result.PlaybackKey, playback, contentType); err != nil {
			return store.Result{}, err
		}
	}
	body, err := json.Marshal(peaks)
	if err != nil {
		return store.Result{}, err
	}
	if err := p.objects.PutBytes(ctx, p.cfg.BucketPeaks, result.PeaksKey, bytes.NewReader(body), int64(len(body)), "application/json"); err != nil {
		return store.Result{}, err
	}
	return result, nil
}

func (p *Processor) fail(ctx context.Context, log *slog.Logger, media store.Media, code, reason string, cause error) {
	log.Error("failed", "code", code, "reason", reason, "error", cause)
	failed, err := p.store.MarkFailed(ctx, media.ID, code, reason)
	if err != nil {
		log.Error("could not record the failure; it will be tried again", "error", err)
		return
	}
	p.publishOutcome(ctx, failed)
}

// retryLater gives the file back. Each attempt waits longer than the one before.
func (p *Processor) retryLater(ctx context.Context, log *slog.Logger, media store.Media, cause error) {
	delay := min(time.Duration(media.Attempts)*p.timing.RetryStep, p.cfg.ClaimTimeout)
	log.Warn("will try again", "in", delay.String(), "error", cause)
	if err := p.store.Release(ctx, media.ID, p.cfg.ClaimTimeout, delay); err != nil {
		log.Error("could not give the file back; it is retried when its claim runs out", "error", err)
	}
}

// publishOutcome sends the ready or failed event. When that does not work the event stays
// marked as unsent and republish sends it later.
func (p *Processor) publishOutcome(ctx context.Context, media store.Media) {
	subject, event := events.ReadySubject, events.Ready(media)
	if media.Status == store.StatusFailed {
		subject, event = events.FailedSubject, events.Failed(media)
	}
	if err := p.bus.Publish(ctx, subject, event); err != nil {
		p.log.Warn("could not publish; will try again", "media_id", media.ID, "subject", subject, "error", err)
		return
	}
	if p.metrics != nil {
		p.metrics.EventsPublished.Add(ctx, 1, metrics.Subject(subject))
	}
	if err := p.store.MarkOutcomePublished(ctx, media.ID); err != nil {
		p.log.Warn("could not record that the event went out", "media_id", media.ID, "error", err)
	}
}

// republish sends the events that could not be sent when their file was finished.
func (p *Processor) republish(ctx context.Context) {
	ticker := time.NewTicker(p.timing.Republish)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		// Only events that have waited a while: a fresh one is still being sent by its worker.
		waiting, err := p.store.Unpublished(ctx, p.timing.Republish, 50)
		if err != nil {
			if ctx.Err() == nil {
				p.log.Error("could not look for unsent events", "error", err)
			}
			continue
		}
		for _, media := range waiting {
			p.publishOutcome(ctx, media)
		}
	}
}

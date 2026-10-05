// Package events publishes this service's events: CloudEvents on NATS JetStream.
//
// Contracts: likho-contracts/events/likho.media.{uploaded,ready,failed}.v1.schema.json and
// streams.yaml for the subjects. Event ids are derived from the media id, so publishing the
// same event again (a retry after a crash) is stored once.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/likho-ai/likho-media/internal/store"
)

// Subjects and names from streams.yaml.
const (
	Source          = "likho-media"
	Stream          = "LIKHO"
	UploadedSubject = "likho.media.uploaded"
	ReadySubject    = "likho.media.ready"
	FailedSubject   = "likho.media.failed"
)

// Event is a CloudEvents 1.0 envelope.
type Event struct {
	SpecVersion     string `json:"specversion"`
	ID              string `json:"id"`
	Source          string `json:"source"`
	Type            string `json:"type"`
	Time            string `json:"time"`
	Subject         string `json:"subject"`
	DataContentType string `json:"datacontenttype"`
	Data            any    `json:"data"`
}

func envelope(id, eventType, subject string, data any) Event {
	return Event{
		SpecVersion:     "1.0",
		ID:              id,
		Source:          Source,
		Type:            eventType,
		Time:            time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Subject:         subject,
		DataContentType: "application/json",
		Data:            data,
	}
}

// Uploaded says a file was stored and waits to be converted.
func Uploaded(m store.Media) Event {
	return envelope("evt_"+m.ID+"_uploaded", "likho.media.uploaded.v1", m.RecordingID, map[string]any{
		"recording_id":  m.RecordingID,
		"media_id":      m.ID,
		"workspace_id":  m.WorkspaceID,
		"original_name": m.OriginalName,
		"sha256":        m.SHA256,
		"size_bytes":    m.SizeBytes,
	})
}

// Ready says the 16 kHz copy and the waveform exist.
func Ready(m store.Media) Event {
	return envelope("evt_"+m.ID+"_ready", "likho.media.ready.v1", m.RecordingID, map[string]any{
		"recording_id":     m.RecordingID,
		"media_id":         m.ID,
		"workspace_id":     m.WorkspaceID,
		"duration_seconds": m.DurationSeconds,
		"channels":         m.Channels,
		"sample_rate":      m.SampleRate,
	})
}

// Failed says the file cannot be converted.
func Failed(m store.Media) Event {
	return envelope("evt_"+m.ID+"_failed", "likho.media.failed.v1", m.RecordingID, map[string]any{
		"recording_id": m.RecordingID,
		"media_id":     m.ID,
		"workspace_id": m.WorkspaceID,
		"code":         m.FailureCode,
		"message":      m.FailureReason,
	})
}

// Bus is the connection to the event bus.
type Bus struct {
	conn *nats.Conn
	js   jetstream.JetStream
}

// Connect opens the connection and checks that the stream exists. While NATS is not there yet
// (it may be starting at the same time) it keeps trying until ctx ends; once connected, the
// client reconnects on its own.
func Connect(ctx context.Context, url string) (*Bus, error) {
	wait := time.Second
	for {
		bus, err := open(ctx, url)
		if err == nil {
			return bus, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("the event bus at %s did not answer in time: %w", url, err)
		case <-timer.C:
		}
		wait = min(wait*2, 10*time.Second)
	}
}

func open(ctx context.Context, url string) (*Bus, error) {
	conn, err := nats.Connect(url, nats.Name(Source), nats.MaxReconnects(-1), nats.Timeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("event bus: %w", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("event bus: %w", err)
	}
	if _, err := js.Stream(ctx, Stream); err != nil {
		conn.Close()
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return nil, fmt.Errorf("stream %s does not exist; create the streams first (likho-infra: scripts/up.sh)", Stream)
		}
		return nil, fmt.Errorf("event bus: %w", err)
	}
	return &Bus{conn: conn, js: js}, nil
}

// Connected reports whether the bus can be reached right now.
func (b *Bus) Connected() bool { return b.conn.IsConnected() }

// Close closes the connection. Every publish was already confirmed, so nothing is waiting.
func (b *Bus) Close() { b.conn.Close() }

// Publish stores the event in the stream and returns when the server has confirmed it.
func (b *Bus) Publish(ctx context.Context, subject string, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = b.js.Publish(ctx, subject, body, jetstream.WithMsgID(event.ID))
	return err
}

package app_test

// The whole service against the likho-infra stack: PostgreSQL, NATS, the object store, FFmpeg.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	mediav1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/media/v1"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/media/v1/mediav1connect"
	"github.com/nats-io/nats.go"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/likho-ai/likho-media/internal/app"
	"github.com/likho-ai/likho-media/internal/audio"
	"github.com/likho-ai/likho-media/internal/config"
	"github.com/likho-ai/likho-media/internal/events"
	"github.com/likho-ai/likho-media/internal/ids"
	"github.com/likho-ai/likho-media/internal/ingest"
	"github.com/likho-ai/likho-media/internal/links"
	"github.com/likho-ai/likho-media/internal/testenv"
)

// ---------------------------------------------------------------- the service under test

type service struct {
	t      *testing.T
	cfg    config.Config
	client mediav1connect.MediaServiceClient
	http   string // http://127.0.0.1:port
	grpc   string
	bus    *nats.Conn
	seen   *eventLog
}

type eventLog struct {
	mu     sync.Mutex
	events []received
}

type received struct {
	subject string
	msgID   string
	body    map[string]any
}

func (l *eventLog) add(message *nats.Msg) {
	var body map[string]any
	if err := json.Unmarshal(message.Data, &body); err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, received{message.Subject, message.Header.Get("Nats-Msg-Id"), body})
}

// about returns the events about one media id, in the order they arrived.
func (l *eventLog) about(mediaID string) []received {
	l.mu.Lock()
	defer l.mu.Unlock()
	var found []received
	for _, event := range l.events {
		if data, ok := event.body["data"].(map[string]any); ok && data["media_id"] == mediaID {
			found = append(found, event)
		}
	}
	return found
}

func start(t *testing.T, options app.Options) *service {
	t.Helper()
	cfg := testenv.Config(t)

	// Listen for the service's events before it starts, so none is missed.
	bus, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	seen := &eventLog{}
	subscription, err := bus.Subscribe("likho.media.*", seen.add)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = subscription.Unsubscribe() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	ctx, stop := context.WithCancel(context.Background())
	instance, err := app.New(ctx, cfg, log, options)
	if err != nil {
		stop()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- instance.Run(ctx) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("the service stopped with an error: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Error("the service did not stop within 30 s")
		}
	})

	grpcURL := "http://" + instance.GRPCAddr()
	return &service{
		t: t, cfg: cfg, bus: bus, seen: seen,
		client: mediav1connect.NewMediaServiceClient(http.DefaultClient, grpcURL),
		http:   "http://" + instance.HTTPAddr(),
		grpc:   grpcURL,
	}
}

var fast = app.Options{Timing: ingest.Timing{IdlePoll: 200 * time.Millisecond, Republish: 300 * time.Millisecond, RetryStep: 100 * time.Millisecond}}

const workspace = "wsp_01JB7Z5K3M9Q2W4X6Y8A0C1E3G"

// ---------------------------------------------------------------- helpers

func (s *service) createUpload(name string) (*mediav1.CreateUploadResponse, string) {
	s.t.Helper()
	recording := ids.New("rec")
	reply, err := s.client.CreateUpload(context.Background(), connect.NewRequest(&mediav1.CreateUploadRequest{
		WorkspaceId: workspace, RecordingId: recording, OriginalName: name,
	}))
	if err != nil {
		s.t.Fatal(err)
	}
	return reply.Msg, recording
}

type uploadAnswer struct {
	status int
	result struct {
		MediaID     string `json:"media_id"`
		SHA256      string `json:"sha256"`
		SizeBytes   int64  `json:"size_bytes"`
		Status      string `json:"status"`
		DuplicateOf string `json:"duplicate_of"`
	}
	problem struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
}

func (s *service) put(link string, body []byte) uploadAnswer {
	s.t.Helper()
	request, err := http.NewRequest(http.MethodPut, link, bytes.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	raw, _ := io.ReadAll(response.Body)
	answer := uploadAnswer{status: response.StatusCode}
	_ = json.Unmarshal(raw, &answer.result)
	_ = json.Unmarshal(raw, &answer.problem)
	return answer
}

// upload sends a file for a new recording and returns the media id and the recording id.
func (s *service) upload(name string, body []byte) (string, string) {
	s.t.Helper()
	created, recording := s.createUpload(name)
	answer := s.put(created.GetUploadUrl(), body)
	if answer.status != http.StatusCreated {
		s.t.Fatalf("upload answered %d %+v", answer.status, answer.problem)
	}
	return created.GetMediaId(), recording
}

func (s *service) media(id string) *mediav1.Media {
	s.t.Helper()
	reply, err := s.client.GetMedia(context.Background(), connect.NewRequest(&mediav1.GetMediaRequest{Id: id}))
	if err != nil {
		s.t.Fatal(err)
	}
	return reply.Msg.GetMedia()
}

func (s *service) waitFor(id string, status mediav1.MediaStatus) *mediav1.Media {
	s.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		media := s.media(id)
		if media.GetStatus() == status {
			return media
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("media %s is %s after 30 s, want %s (%s)", id, media.GetStatus(), status, media.GetFailureReason())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// waitForEvents returns the events about a media id once there are at least count of them.
func (s *service) waitForEvents(id string, count int) []received {
	s.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		found := s.seen.about(id)
		if len(found) >= count {
			return found
		}
		if time.Now().After(deadline) {
			s.t.Fatalf("%d events about %s after 15 s, want %d", len(found), id, count)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *service) link(id string, kind mediav1.MediaKind) string {
	s.t.Helper()
	reply, err := s.client.GetDownloadUrl(context.Background(), connect.NewRequest(&mediav1.GetDownloadUrlRequest{Id: id, Kind: kind}))
	if err != nil {
		s.t.Fatal(err)
	}
	return reply.Msg.GetUrl()
}

func get(t *testing.T, link string, header ...string) (*http.Response, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, link, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		request.Header.Set(header[i], header[i+1])
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response, body
}

func errorCode(body []byte) string {
	var problem struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(body, &problem)
	return problem.Error.Code
}

func tone(t *testing.T, s *service, ext string) []byte {
	t.Helper()
	body, err := os.ReadFile(testenv.Tone(t, s.cfg, ext, "", 2, 2, 44100))
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// unique makes generated audio different from every other test's, so no test sees another's duplicate.
func unique(body []byte) []byte {
	return append(slices.Clone(body), []byte(ids.New("pad"))...)
}

func wantCode(t *testing.T, err error, code connect.Code) {
	t.Helper()
	if connect.CodeOf(err) != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}

var schemas = sync.OnceValue(func() *jsonschema.Compiler {
	compiler := jsonschema.NewCompiler()
	paths, _ := filepath.Glob("testdata/contracts/*.json")
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}
		document, err := jsonschema.UnmarshalJSON(file)
		_ = file.Close()
		if err != nil {
			panic(err)
		}
		if err := compiler.AddResource(filepath.Base(path), document); err != nil {
			panic(err)
		}
	}
	return compiler
})

// matchesContract checks an event against the schemas copied from likho-contracts.
func matchesContract(t *testing.T, event received, eventType string) {
	t.Helper()
	for name, value := range map[string]any{"cloudevent.schema.json": event.body, eventType + ".schema.json": event.body["data"]} {
		schema, err := schemas().Compile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(value); err != nil {
			t.Fatalf("%s does not match %s: %v", event.subject, name, err)
		}
	}
	if event.body["type"] != eventType {
		t.Fatalf("event type %v, want %s", event.body["type"], eventType)
	}
	if event.msgID != event.body["id"] {
		t.Fatalf("Nats-Msg-Id %q differs from the event id %v", event.msgID, event.body["id"])
	}
}

// ---------------------------------------------------------------- tests

func TestAnUploadBecomesAudioThePlayerCanUse(t *testing.T) {
	s := start(t, fast)
	body := unique(tone(t, s, ".wav"))
	created, recording := s.createUpload("Call 01.WAV")

	if created.GetExistingMedia() != nil || !ids.Valid(created.GetMediaId(), "med") {
		t.Fatalf("unexpected answer %+v", created)
	}
	if until := time.Until(created.GetExpiresAt().AsTime()); until < 50*time.Minute || until > time.Hour {
		t.Fatalf("the upload link expires in %s, want about an hour", until)
	}

	answer := s.put(created.GetUploadUrl(), body)
	sum := sha256.Sum256(body)
	if answer.status != http.StatusCreated || answer.result.MediaID != created.GetMediaId() ||
		answer.result.SHA256 != hex.EncodeToString(sum[:]) || answer.result.SizeBytes != int64(len(body)) ||
		answer.result.Status != "uploaded" || answer.result.DuplicateOf != "" {
		t.Fatalf("upload answered %d %+v", answer.status, answer.result)
	}

	media := s.waitFor(created.GetMediaId(), mediav1.MediaStatus_MEDIA_STATUS_READY)
	if media.GetChannels() != 2 || media.GetSampleRate() != 44100 || math.Abs(media.GetDurationSeconds()-2) > 0.1 {
		t.Errorf("the file is described as %d channels, %d Hz, %.2f s", media.GetChannels(), media.GetSampleRate(), media.GetDurationSeconds())
	}
	if media.GetOriginalName() != "Call 01.WAV" || media.GetWorkspaceId() != workspace || media.GetRecordingId() != recording ||
		media.GetSizeBytes() != uint64(len(body)) || media.GetSha256() != hex.EncodeToString(sum[:]) || media.GetFailureReason() != "" {
		t.Errorf("unexpected media %+v", media)
	}

	t.Run("events", func(t *testing.T) {
		found := s.waitForEvents(media.GetId(), 2)
		if len(found) != 2 || found[0].subject != events.UploadedSubject || found[1].subject != events.ReadySubject {
			t.Fatalf("got %d events %v, want uploaded then ready", len(found), found)
		}
		matchesContract(t, found[0], "likho.media.uploaded.v1")
		matchesContract(t, found[1], "likho.media.ready.v1")
		ready := found[1].body["data"].(map[string]any)
		if ready["recording_id"] != recording || ready["channels"] != float64(2) || ready["sample_rate"] != float64(44100) {
			t.Fatalf("ready event says %v", ready)
		}
		if found[1].body["subject"] != recording || found[1].body["source"] != "likho-media" {
			t.Fatalf("unexpected envelope %v", found[1].body)
		}
	})

	t.Run("what the browser plays", func(t *testing.T) {
		// A browser plays this file as it is, so no copy was made: the link leads to the original.
		link := s.link(media.GetId(), mediav1.MediaKind_MEDIA_KIND_NORMALIZED)
		response, played := get(t, link)
		if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "audio/wav" || !bytes.Equal(played, body) {
			t.Fatalf("got %d %s, %d bytes", response.StatusCode, response.Header.Get("Content-Type"), len(played))
		}

		// A player jumps to a line by asking for a part of the file.
		part, some := get(t, link, "Range", "bytes=100-199")
		if part.StatusCode != http.StatusPartialContent || !bytes.Equal(some, played[100:200]) ||
			part.Header.Get("Content-Range") != fmt.Sprintf("bytes 100-199/%d", len(played)) {
			t.Fatalf("range request: %d, %d bytes, Content-Range %q", part.StatusCode, len(some), part.Header.Get("Content-Range"))
		}
		if response.Header.Get("Accept-Ranges") != "bytes" || response.Header.Get("ETag") == "" {
			t.Fatalf("headers %v", response.Header)
		}
	})

	t.Run("the waveform", func(t *testing.T) {
		response, body := get(t, s.link(media.GetId(), mediav1.MediaKind_MEDIA_KIND_PEAKS))
		var peaks audio.Peaks
		if err := json.Unmarshal(body, &peaks); err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("got %d, %v", response.StatusCode, err)
		}
		if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
			t.Fatalf("content type %s", response.Header.Get("Content-Type"))
		}
		if n := len(peaks.Peaks); n < 39 || n > 42 || slices.Max(peaks.Peaks) < 5 || peaks.Max != 100 {
			t.Fatalf("unexpected waveform %+v", peaks)
		}
	})

	t.Run("the original", func(t *testing.T) {
		response, original := get(t, s.link(media.GetId(), mediav1.MediaKind_MEDIA_KIND_ORIGINAL))
		if response.StatusCode != http.StatusOK || !bytes.Equal(original, body) {
			t.Fatalf("got %d and %d bytes, want the %d bytes that were uploaded", response.StatusCode, len(original), len(body))
		}
	})
}

func TestAnMP3IsPlayedAsItIs(t *testing.T) {
	s := start(t, fast)
	body := unique(tone(t, s, ".mp3"))
	// The uploader did not say what the file is; the service finds out.
	id, _ := s.upload("call.mp3", body)
	media := s.waitFor(id, mediav1.MediaStatus_MEDIA_STATUS_READY)
	if media.GetChannels() != 2 || math.Abs(media.GetDurationSeconds()-2) > 0.2 {
		t.Fatalf("unexpected media %+v", media)
	}
	response, played := get(t, s.link(id, mediav1.MediaKind_MEDIA_KIND_NORMALIZED))
	if response.Header.Get("Content-Type") != "audio/mpeg" || !bytes.Equal(played, body) {
		t.Fatalf("got %s and %d bytes, want the MP3 that was uploaded", response.Header.Get("Content-Type"), len(played))
	}
}

// Telephone recordings are often A-law or mu-law WAV files, which browsers do not play.
func TestATelephoneCodecGetsACopyTheBrowserCanPlay(t *testing.T) {
	s := start(t, fast)
	source, err := os.ReadFile(testenv.Tone(t, s.cfg, ".wav", "pcm_mulaw", 2, 1, 8000))
	if err != nil {
		t.Fatal(err)
	}
	body := unique(source)
	id, _ := s.upload("call.wav", body)
	media := s.waitFor(id, mediav1.MediaStatus_MEDIA_STATUS_READY)
	if media.GetChannels() != 1 || media.GetSampleRate() != 8000 {
		t.Fatalf("unexpected media %+v", media)
	}

	response, played := get(t, s.link(id, mediav1.MediaKind_MEDIA_KIND_NORMALIZED))
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "audio/mpeg" {
		t.Fatalf("got %d %s", response.StatusCode, response.Header.Get("Content-Type"))
	}
	path := filepath.Join(t.TempDir(), "playback.mp3")
	if err := os.WriteFile(path, played, 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := audio.Tools{FFmpeg: s.cfg.FFmpegPath, FFprobe: s.cfg.FFprobePath}.Probe(context.Background(), path)
	if err != nil || info.Codec != "mp3" || math.Abs(info.DurationSeconds-2) > 0.2 {
		t.Fatalf("the copy is %+v (%v), want an MP3 of 2 s", info, err)
	}

	// The speech model still gets the file exactly as it was uploaded.
	if _, original := get(t, s.link(id, mediav1.MediaKind_MEDIA_KIND_ORIGINAL)); !bytes.Equal(original, body) {
		t.Fatalf("the original is %d bytes, want the %d that were uploaded", len(original), len(body))
	}

	// Deleting removes the copy too.
	link := s.link(id, mediav1.MediaKind_MEDIA_KIND_NORMALIZED)
	if _, err := s.client.DeleteMedia(context.Background(), connect.NewRequest(&mediav1.DeleteMediaRequest{Id: id})); err != nil {
		t.Fatal(err)
	}
	if response, _ := get(t, link); response.StatusCode != http.StatusNotFound {
		t.Fatalf("after the delete the link answers %d", response.StatusCode)
	}
}

func TestAFileThatIsNotAudioFails(t *testing.T) {
	s := start(t, fast)
	id, recording := s.upload("notes.mp3", []byte("this is not audio, whatever the name says "+ids.New("pad")))

	media := s.waitFor(id, mediav1.MediaStatus_MEDIA_STATUS_FAILED)
	if media.GetFailureReason() != ingest.ReasonUnreadable {
		t.Fatalf("reason %q", media.GetFailureReason())
	}
	found := s.waitForEvents(id, 2)
	if found[1].subject != events.FailedSubject {
		t.Fatalf("second event is %s, want failed", found[1].subject)
	}
	matchesContract(t, found[1], "likho.media.failed.v1")
	data := found[1].body["data"].(map[string]any)
	if data["code"] != "audio_unreadable" || data["message"] != ingest.ReasonUnreadable || data["recording_id"] != recording {
		t.Fatalf("failed event says %v", data)
	}

	// There is no 16 kHz copy to hand out, and the caller is told why.
	_, err := s.client.GetDownloadUrl(context.Background(), connect.NewRequest(&mediav1.GetDownloadUrlRequest{
		Id: id, Kind: mediav1.MediaKind_MEDIA_KIND_NORMALIZED,
	}))
	wantCode(t, err, connect.CodeFailedPrecondition)
	if !strings.Contains(err.Error(), ingest.ReasonUnreadable) {
		t.Fatalf("error %q does not say why", err)
	}
	// What was uploaded can still be looked at.
	if response, _ := get(t, s.link(id, mediav1.MediaKind_MEDIA_KIND_ORIGINAL)); response.StatusCode != http.StatusOK {
		t.Fatalf("original: got %d", response.StatusCode)
	}
}

func TestTheSameLinkTwice(t *testing.T) {
	s := start(t, fast)
	body := unique(tone(t, s, ".wav"))
	created, _ := s.createUpload("call.wav")
	if answer := s.put(created.GetUploadUrl(), body); answer.status != http.StatusCreated {
		t.Fatalf("first upload: %d", answer.status)
	}

	again := s.put(created.GetUploadUrl(), body)
	if again.status != http.StatusOK || again.result.MediaID != created.GetMediaId() {
		t.Fatalf("the same file again: %d %+v, want 200 and the same media", again.status, again.result)
	}
	other := s.put(created.GetUploadUrl(), unique(body))
	if other.status != http.StatusConflict || other.problem.Error.Code != "already_uploaded" {
		t.Fatalf("a different file: %d %+v, want 409 already_uploaded", other.status, other.problem)
	}
	s.waitFor(created.GetMediaId(), mediav1.MediaStatus_MEDIA_STATUS_READY)
	if found := s.waitForEvents(created.GetMediaId(), 2); len(found) != 2 {
		t.Fatalf("%d events, want 2: the repeated upload must not announce the file again", len(found))
	}
}

func TestTheSameContentAgain(t *testing.T) {
	s := start(t, fast)
	body := unique(tone(t, s, ".wav"))
	sum := sha256.Sum256(body)
	first, _ := s.upload("call.wav", body)

	t.Run("a second upload is stored and says which file it repeats", func(t *testing.T) {
		created, _ := s.createUpload("call copy.wav")
		answer := s.put(created.GetUploadUrl(), body)
		if answer.status != http.StatusCreated || answer.result.DuplicateOf != first {
			t.Fatalf("got %d %+v, want duplicate_of %s", answer.status, answer.result, first)
		}
	})

	t.Run("a caller that knows the checksum is spared the upload", func(t *testing.T) {
		reply, err := s.client.CreateUpload(context.Background(), connect.NewRequest(&mediav1.CreateUploadRequest{
			WorkspaceId: workspace, RecordingId: ids.New("rec"), OriginalName: "call.wav", Sha256: hex.EncodeToString(sum[:]),
		}))
		if err != nil {
			t.Fatal(err)
		}
		if reply.Msg.GetUploadUrl() != "" || reply.Msg.GetMediaId() != first || reply.Msg.GetExistingMedia().GetId() != first {
			t.Fatalf("unexpected answer %+v", reply.Msg)
		}
	})

	t.Run("another workspace does not see it", func(t *testing.T) {
		reply, err := s.client.CreateUpload(context.Background(), connect.NewRequest(&mediav1.CreateUploadRequest{
			WorkspaceId: "wsp_01JB7Z5K3M9Q2W4X6Y8A0C1E99", RecordingId: ids.New("rec"), OriginalName: "call.wav",
			Sha256: hex.EncodeToString(sum[:]),
		}))
		if err != nil || reply.Msg.GetExistingMedia() != nil || reply.Msg.GetUploadUrl() == "" {
			t.Fatalf("got %+v, %v; want a normal upload link", reply.Msg, err)
		}
	})
}

func TestLinksAreChecked(t *testing.T) {
	s := start(t, fast)
	body := unique(tone(t, s, ".wav"))
	created, _ := s.createUpload("call.wav")
	uploadLink, _ := url.Parse(created.GetUploadUrl())

	t.Run("an upload link that was changed", func(t *testing.T) {
		changed := *uploadLink
		changed.RawQuery = "token=" + uploadLink.Query().Get("token") + "x"
		if answer := s.put(changed.String(), body); answer.status != http.StatusForbidden || answer.problem.Error.Code != "link_invalid" {
			t.Fatalf("got %d %+v", answer.status, answer.problem)
		}
	})
	t.Run("an upload link used for another media id", func(t *testing.T) {
		other := *uploadLink
		other.Path = "/media/uploads/" + ids.New("med")
		if answer := s.put(other.String(), body); answer.status != http.StatusForbidden {
			t.Fatalf("got %d", answer.status)
		}
	})
	t.Run("an upload link without a token", func(t *testing.T) {
		if answer := s.put(s.http+"/media/uploads/"+created.GetMediaId(), body); answer.status != http.StatusForbidden {
			t.Fatalf("got %d", answer.status)
		}
	})
	t.Run("an upload link that has expired", func(t *testing.T) {
		signer := links.NewSigner(s.cfg.LinkSecret, s.http)
		signer.SetClock(func() time.Time { return time.Now().Add(-2 * time.Hour) })
		old, _ := signer.UploadURL(links.Upload{
			MediaID: ids.New("med"), WorkspaceID: workspace, RecordingID: ids.New("rec"), OriginalName: "call.wav",
		}, time.Hour)
		if answer := s.put(old, body); answer.status != http.StatusForbidden || answer.problem.Error.Code != "link_expired" {
			t.Fatalf("got %d %+v", answer.status, answer.problem)
		}
	})
	t.Run("nothing was stored by any of them", func(t *testing.T) {
		_, err := s.client.GetMedia(context.Background(), connect.NewRequest(&mediav1.GetMediaRequest{Id: created.GetMediaId()}))
		wantCode(t, err, connect.CodeNotFound)
	})

	if answer := s.put(created.GetUploadUrl(), body); answer.status != http.StatusCreated {
		t.Fatalf("the real link: got %d", answer.status)
	}
	s.waitFor(created.GetMediaId(), mediav1.MediaStatus_MEDIA_STATUS_READY)
	audioLink, _ := url.Parse(s.link(created.GetMediaId(), mediav1.MediaKind_MEDIA_KIND_NORMALIZED))

	t.Run("a download link for one kind does not open another", func(t *testing.T) {
		other := *audioLink
		other.Path = strings.TrimSuffix(other.Path, "/audio") + "/original"
		if response, raw := get(t, other.String()); response.StatusCode != http.StatusForbidden || errorCode(raw) != "link_invalid" {
			t.Fatalf("got %d %s", response.StatusCode, raw)
		}
	})
	t.Run("a download link without a signature", func(t *testing.T) {
		bare := *audioLink
		bare.RawQuery = ""
		if response, _ := get(t, bare.String()); response.StatusCode != http.StatusForbidden {
			t.Fatalf("got %d", response.StatusCode)
		}
	})
	t.Run("a download link that has expired", func(t *testing.T) {
		signer := links.NewSigner(s.cfg.LinkSecret, s.http)
		signer.SetClock(func() time.Time { return time.Now().Add(-time.Hour) })
		old, _ := signer.DownloadURL(created.GetMediaId(), links.KindAudio, time.Minute)
		if response, raw := get(t, old); response.StatusCode != http.StatusForbidden || errorCode(raw) != "link_expired" {
			t.Fatalf("got %d %s", response.StatusCode, raw)
		}
	})
	t.Run("a path that is not a media id", func(t *testing.T) {
		if response, _ := get(t, s.http+"/media/..%2F..%2Fetc/audio"); response.StatusCode != http.StatusNotFound {
			t.Fatalf("got %d", response.StatusCode)
		}
	})
}

func TestUploadsThatAreRefused(t *testing.T) {
	s := start(t, fast) // the test configuration allows 1 MB

	t.Run("too large", func(t *testing.T) {
		created, _ := s.createUpload("long call.wav")
		answer := s.put(created.GetUploadUrl(), make([]byte, 1<<20+1))
		if answer.status != http.StatusRequestEntityTooLarge || answer.problem.Error.Code != "too_large" {
			t.Fatalf("got %d %+v", answer.status, answer.problem)
		}
		_, err := s.client.GetMedia(context.Background(), connect.NewRequest(&mediav1.GetMediaRequest{Id: created.GetMediaId()}))
		wantCode(t, err, connect.CodeNotFound)
	})
	t.Run("empty", func(t *testing.T) {
		created, _ := s.createUpload("nothing.wav")
		if answer := s.put(created.GetUploadUrl(), nil); answer.status != http.StatusBadRequest || answer.problem.Error.Code != "empty_file" {
			t.Fatalf("got %d %+v", answer.status, answer.problem)
		}
	})

	bad := map[string]*mediav1.CreateUploadRequest{
		"no workspace":                   {RecordingId: ids.New("rec"), OriginalName: "a.wav"},
		"no recording":                   {WorkspaceId: workspace, OriginalName: "a.wav"},
		"a recording id that is not one": {WorkspaceId: workspace, RecordingId: "../x", OriginalName: "a.wav"},
		"no name":                        {WorkspaceId: workspace, RecordingId: ids.New("rec"), OriginalName: "  "},
		"a checksum that is not one":     {WorkspaceId: workspace, RecordingId: ids.New("rec"), OriginalName: "a.wav", Sha256: "abc"},
		"a declared size over the limit": {WorkspaceId: workspace, RecordingId: ids.New("rec"), OriginalName: "a.wav", SizeBytes: 2 << 20},
	}
	for name, request := range bad {
		t.Run(name, func(t *testing.T) {
			_, err := s.client.CreateUpload(context.Background(), connect.NewRequest(request))
			wantCode(t, err, connect.CodeInvalidArgument)
		})
	}
}

func TestQuestionsAboutFilesThatDoNotExist(t *testing.T) {
	s := start(t, fast)
	ctx := context.Background()
	missing := ids.New("med")

	_, err := s.client.GetMedia(ctx, connect.NewRequest(&mediav1.GetMediaRequest{Id: missing}))
	wantCode(t, err, connect.CodeNotFound)
	_, err = s.client.GetMedia(ctx, connect.NewRequest(&mediav1.GetMediaRequest{}))
	wantCode(t, err, connect.CodeInvalidArgument)
	_, err = s.client.GetDownloadUrl(ctx, connect.NewRequest(&mediav1.GetDownloadUrlRequest{Id: missing, Kind: mediav1.MediaKind_MEDIA_KIND_NORMALIZED}))
	wantCode(t, err, connect.CodeNotFound)
	_, err = s.client.GetDownloadUrl(ctx, connect.NewRequest(&mediav1.GetDownloadUrlRequest{Id: missing}))
	wantCode(t, err, connect.CodeInvalidArgument)

	// A link that is correctly signed for a file that is gone.
	signer := links.NewSigner(s.cfg.LinkSecret, s.http)
	link, _ := signer.DownloadURL(missing, links.KindAudio, time.Minute)
	if response, raw := get(t, link); response.StatusCode != http.StatusNotFound || errorCode(raw) != "not_found" {
		t.Fatalf("got %d %s", response.StatusCode, raw)
	}
}

func TestDeleteRemovesEverything(t *testing.T) {
	s := start(t, fast)
	ctx := context.Background()
	id, _ := s.upload("call.wav", unique(tone(t, s, ".wav")))
	s.waitFor(id, mediav1.MediaStatus_MEDIA_STATUS_READY)
	audioLink := s.link(id, mediav1.MediaKind_MEDIA_KIND_NORMALIZED)

	if _, err := s.client.DeleteMedia(ctx, connect.NewRequest(&mediav1.DeleteMediaRequest{Id: id})); err != nil {
		t.Fatal(err)
	}
	_, err := s.client.GetMedia(ctx, connect.NewRequest(&mediav1.GetMediaRequest{Id: id}))
	wantCode(t, err, connect.CodeNotFound)
	if response, _ := get(t, audioLink); response.StatusCode != http.StatusNotFound {
		t.Fatalf("a link handed out before the delete still answers %d", response.StatusCode)
	}
	if _, err := s.client.DeleteMedia(ctx, connect.NewRequest(&mediav1.DeleteMediaRequest{Id: id})); err != nil {
		t.Fatalf("deleting twice: %v", err)
	}
	_, err = s.client.DeleteMedia(ctx, connect.NewRequest(&mediav1.DeleteMediaRequest{}))
	wantCode(t, err, connect.CodeInvalidArgument)
}

// flaky fails the first publishes on chosen subjects, like an event bus that is restarting.
type flaky struct {
	real     ingest.Publisher
	mu       sync.Mutex
	failures map[string]int
}

func (f *flaky) Publish(ctx context.Context, subject string, event events.Event) error {
	f.mu.Lock()
	left := f.failures[subject]
	if left > 0 {
		f.failures[subject] = left - 1
	}
	f.mu.Unlock()
	if left > 0 {
		return errors.New("the event bus is restarting")
	}
	return f.real.Publish(ctx, subject, event)
}

func TestEventsAreSentLaterWhenTheBusWasDown(t *testing.T) {
	options := fast
	options.Publisher = func(real *events.Bus) ingest.Publisher {
		return &flaky{real: real, failures: map[string]int{events.UploadedSubject: 1, events.ReadySubject: 2}}
	}
	s := start(t, options)
	id, _ := s.upload("call.wav", unique(tone(t, s, ".wav")))

	// The file becomes ready although nothing could be announced at first...
	s.waitFor(id, mediav1.MediaStatus_MEDIA_STATUS_READY)
	// ...and both events arrive once the bus answers again, each exactly once.
	s.waitForEvents(id, 2)
	time.Sleep(time.Second)
	found := s.seen.about(id)
	if len(found) != 2 || found[0].subject != events.UploadedSubject || found[1].subject != events.ReadySubject {
		t.Fatalf("got %d events %v, want uploaded then ready, once each", len(found), found)
	}
}

func TestSeveralFilesAtOnce(t *testing.T) {
	s := start(t, fast)
	base := tone(t, s, ".wav")
	var uploaded []string
	for i := range 6 {
		id, _ := s.upload(fmt.Sprintf("call %d.wav", i), unique(base))
		uploaded = append(uploaded, id)
	}
	for _, id := range uploaded {
		s.waitFor(id, mediav1.MediaStatus_MEDIA_STATUS_READY)
		if found := s.waitForEvents(id, 2); len(found) != 2 {
			t.Fatalf("%s: %d events, want 2", id, len(found))
		}
	}
}

func TestHealth(t *testing.T) {
	s := start(t, fast)
	for path, want := range map[string]string{"/healthz": "ok\n", "/readyz": "ready\n"} {
		if response, body := get(t, s.http+path); response.StatusCode != http.StatusOK || string(body) != want {
			t.Fatalf("%s: got %d %q", path, response.StatusCode, body)
		}
	}
	if response, _ := get(t, s.http+"/nothing-here"); response.StatusCode != http.StatusNotFound {
		t.Fatalf("got %d", response.StatusCode)
	}
	// Metrics: Prometheus text with the media by status, after the conversions the other tests made.
	response, body := get(t, s.http+"/metrics")
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "likho_media{") {
		t.Fatalf("/metrics: got %d %q", response.StatusCode, body)
	}
}

// The Python services call this one with ordinary gRPC clients: HTTP/2 without TLS.
func TestItAnswersPlainGRPC(t *testing.T) {
	s := start(t, fast)
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	h2c := &http.Client{Transport: &http.Transport{Protocols: protocols}}
	t.Cleanup(h2c.CloseIdleConnections)

	client := mediav1connect.NewMediaServiceClient(h2c, s.grpc, connect.WithGRPC())
	_, err := client.GetMedia(context.Background(), connect.NewRequest(&mediav1.GetMediaRequest{Id: ids.New("med")}))
	wantCode(t, err, connect.CodeNotFound)

	health := grpchealth.NewClient(h2c, s.grpc, connect.WithGRPC())
	reply, err := health.Check(context.Background(), &grpchealth.CheckRequest{Service: mediav1connect.MediaServiceName})
	if err != nil || reply.Status != grpchealth.StatusServing {
		t.Fatalf("health: %+v, %v", reply, err)
	}
}

func TestItRefusesToStartWithoutItsBuckets(t *testing.T) {
	cfg := testenv.Config(t)
	cfg.BucketPeaks = "no-such-bucket"
	_, err := app.New(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), app.Options{})
	if err == nil || !strings.Contains(err.Error(), "no-such-bucket does not exist") {
		t.Fatalf("got %v", err)
	}
}

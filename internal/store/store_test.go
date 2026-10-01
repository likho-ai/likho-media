package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/likho-ai/likho-media/internal/ids"
	"github.com/likho-ai/likho-media/internal/store"
	"github.com/likho-ai/likho-media/internal/testenv"
)

const workspace = "wsp_01JB7Z5K3M9Q2W4X6Y8A0C1E3G"

func open(t *testing.T) *store.Store {
	t.Helper()
	cfg := testenv.Config(t)
	db, err := store.Open(context.Background(), cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return db
}

func file(sha string) store.Media {
	id := ids.New("med")
	return store.Media{
		ID: id, WorkspaceID: workspace, RecordingID: ids.New("rec"), OriginalName: "call.wav",
		ContentType: "audio/wav", SizeBytes: 1234, SHA256: sha, OriginalKey: workspace + "/" + id + "/original.wav",
	}
}

func insert(t *testing.T, db *store.Store, m store.Media) store.Media {
	t.Helper()
	stored, err := db.Insert(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}

func TestMigrateTwiceIsHarmless(t *testing.T) {
	db := open(t)
	if err := db.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInsertAndGet(t *testing.T) {
	db, ctx := open(t), context.Background()
	stored := insert(t, db, file("aa"))
	if stored.Status != store.StatusUploaded || stored.CreatedAt.IsZero() || stored.UploadedPublished {
		t.Fatalf("unexpected new row %+v", stored)
	}
	got, err := db.Get(ctx, stored.ID)
	if err != nil || got != stored {
		t.Fatalf("got %+v, %v; want %+v", got, err, stored)
	}
	if _, err := db.Insert(ctx, stored); !errors.Is(err, store.ErrExists) {
		t.Fatalf("second insert: got %v, want ErrExists", err)
	}
	if _, err := db.Get(ctx, ids.New("med")); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown id: got %v, want ErrNotFound", err)
	}
}

func TestAFileIsClaimedByOneWorkerAtATime(t *testing.T) {
	db, ctx := open(t), context.Background()
	first := insert(t, db, file("aa"))
	second := insert(t, db, file("bb"))

	claimed, err := db.Claim(ctx, time.Minute)
	if err != nil || claimed.ID != first.ID || claimed.Attempts != 1 {
		t.Fatalf("got %+v, %v; want the oldest file, attempt 1", claimed, err)
	}
	claimed, err = db.Claim(ctx, time.Minute)
	if err != nil || claimed.ID != second.ID {
		t.Fatalf("got %+v, %v; want the second file", claimed, err)
	}
	if _, err := db.Claim(ctx, time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("both files are held: got %v, want ErrNotFound", err)
	}
}

func TestAFileWhoseWorkerDiedIsClaimedAgain(t *testing.T) {
	db, ctx := open(t), context.Background()
	stored := insert(t, db, file("aa"))
	if _, err := db.Claim(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	// With a claim timeout of zero, the claim above has already run out.
	again, err := db.Claim(ctx, 0)
	if err != nil || again.ID != stored.ID || again.Attempts != 2 {
		t.Fatalf("got %+v, %v; want the same file, attempt 2", again, err)
	}
}

func TestAReleasedFileComesBackAfterTheDelay(t *testing.T) {
	db, ctx := open(t), context.Background()
	stored := insert(t, db, file("aa"))
	if _, err := db.Claim(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := db.Release(ctx, stored.ID, time.Minute, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Claim(ctx, time.Minute); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("before the delay: got %v, want ErrNotFound", err)
	}
	time.Sleep(400 * time.Millisecond)
	again, err := db.Claim(ctx, time.Minute)
	if err != nil || again.ID != stored.ID {
		t.Fatalf("after the delay: got %+v, %v", again, err)
	}
}

func TestReadyAndFailedFilesLeaveTheQueue(t *testing.T) {
	db, ctx := open(t), context.Background()
	good, bad := insert(t, db, file("aa")), insert(t, db, file("bb"))

	ready, err := db.MarkReady(ctx, good.ID, store.Result{
		PlaybackKey: "n", PlaybackContentType: "audio/mpeg", PeaksKey: "p", DurationSeconds: 61.5, Channels: 2,
		SampleRate: 8000, Codec: "pcm_mulaw",
	})
	if err != nil || ready.Status != store.StatusReady || ready.DurationSeconds != 61.5 || ready.Channels != 2 ||
		ready.Codec != "pcm_mulaw" || ready.PlaybackKey != "n" || ready.PlaybackContentType != "audio/mpeg" {
		t.Fatalf("got %+v, %v", ready, err)
	}
	failed, err := db.MarkFailed(ctx, bad.ID, "audio_unreadable", "The file is not audio that can be read")
	if err != nil || failed.Status != store.StatusFailed || failed.FailureCode != "audio_unreadable" {
		t.Fatalf("got %+v, %v", failed, err)
	}
	if _, err := db.Claim(ctx, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("got %v, want an empty queue", err)
	}
	// A result arrives once: a second worker that finishes late changes nothing.
	if _, err := db.MarkFailed(ctx, good.ID, "internal", "late"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound for a file that is already ready", err)
	}
}

func TestUnsentEventsAreFoundUntilTheyAreSent(t *testing.T) {
	db, ctx := open(t), context.Background()
	stored := insert(t, db, file("aa"))
	if waiting, _ := db.Unpublished(ctx, 0, 10); len(waiting) != 0 {
		t.Fatalf("a file that is still waiting has no outcome to send, got %d", len(waiting))
	}
	if _, err := db.MarkReady(ctx, stored.ID, store.Result{PeaksKey: "p"}); err != nil {
		t.Fatal(err)
	}
	if waiting, _ := db.Unpublished(ctx, time.Hour, 10); len(waiting) != 0 {
		t.Fatalf("a fresh outcome is still being sent by its worker, got %d", len(waiting))
	}
	waiting, err := db.Unpublished(ctx, 0, 10)
	if err != nil || len(waiting) != 1 || waiting[0].ID != stored.ID {
		t.Fatalf("got %+v, %v", waiting, err)
	}
	if err := db.MarkOutcomePublished(ctx, stored.ID); err != nil {
		t.Fatal(err)
	}
	if waiting, _ := db.Unpublished(ctx, 0, 10); len(waiting) != 0 {
		t.Fatalf("the event was sent, got %d still waiting", len(waiting))
	}
}

func TestFindBySHA256(t *testing.T) {
	db, ctx := open(t), context.Background()
	first := insert(t, db, file("aa"))
	second := insert(t, db, file("aa"))
	elsewhere := file("aa")
	elsewhere.WorkspaceID = "wsp_01JB7Z5K3M9Q2W4X6Y8A0C1E99"
	insert(t, db, elsewhere)

	found, err := db.FindBySHA256(ctx, workspace, "aa", "")
	if err != nil || found.ID != first.ID {
		t.Fatalf("got %+v, %v; want the oldest", found, err)
	}
	found, err = db.FindBySHA256(ctx, workspace, "aa", first.ID)
	if err != nil || found.ID != second.ID {
		t.Fatalf("got %+v, %v; want the other one", found, err)
	}
	if _, err := db.FindBySHA256(ctx, workspace, "cc", ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("new content: got %v, want ErrNotFound", err)
	}
	// Content that could not be read is not a reason to skip an upload.
	if _, err := db.MarkFailed(ctx, first.ID, "audio_unreadable", "x"); err != nil {
		t.Fatal(err)
	}
	found, err = db.FindBySHA256(ctx, workspace, "aa", "")
	if err != nil || found.ID != second.ID {
		t.Fatalf("got %+v, %v; want the one that did not fail", found, err)
	}
}

func TestDelete(t *testing.T) {
	db, ctx := open(t), context.Background()
	stored := insert(t, db, file("aa"))
	deleted, err := db.Delete(ctx, stored.ID)
	if err != nil || deleted.OriginalKey != stored.OriginalKey {
		t.Fatalf("got %+v, %v", deleted, err)
	}
	if _, err := db.Delete(ctx, stored.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("second delete: got %v, want ErrNotFound", err)
	}
}

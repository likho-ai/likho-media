// Package httpapi is the part of the service browsers and workers talk to over plain HTTP:
// sending a file to an upload link, and reading audio and waveforms from download links.
package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/likho-ai/likho-media/internal/config"
	"github.com/likho-ai/likho-media/internal/ids"
	"github.com/likho-ai/likho-media/internal/links"
	"github.com/likho-ai/likho-media/internal/objects"
	"github.com/likho-ai/likho-media/internal/store"
)

// API serves the HTTP routes.
type API struct {
	cfg     config.Config
	store   *store.Store
	objects *objects.Store
	signer  *links.Signer
	wake    func()
	ready   func(context.Context) bool
	log     *slog.Logger
}

// New returns the API. wake is called when a file has arrived; ready answers /readyz.
func New(cfg config.Config, db *store.Store, objs *objects.Store, signer *links.Signer, wake func(),
	ready func(context.Context) bool, log *slog.Logger) *API {
	return &API{cfg: cfg, store: db, objects: objs, signer: signer, wake: wake, ready: ready, log: log}
}

// Handler returns the routes.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if !a.ready(ctx) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "not ready\n")
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	mux.HandleFunc("PUT /media/uploads/{id}", a.upload)
	mux.HandleFunc("GET /media/{id}/{kind}", a.download)
	return mux
}

// The error shape every Likho HTTP API uses.
func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func linkError(w http.ResponseWriter, err error) {
	if errors.Is(err, links.ErrExpired) {
		writeError(w, http.StatusForbidden, "link_expired", "This link has expired. Ask for a new one.")
		return
	}
	writeError(w, http.StatusForbidden, "link_invalid", "This link is not valid.")
}

// UploadResult is the answer to a finished upload.
type UploadResult struct {
	MediaID   string `json:"media_id"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
	Status    string `json:"status"`
	// DuplicateOf is the id of an earlier file in the workspace with the same content, if any.
	DuplicateOf string `json:"duplicate_of,omitempty"`
}

// storeTimeout is how long storing a received file may take before the upload is answered with an error.
const storeTimeout = 2 * time.Minute

var extension = regexp.MustCompile(`^\.[a-z0-9]{1,8}$`)

// objectKey is where a file's original is kept: workspace/media id/original.ext
func objectKey(upload links.Upload) string {
	ext := strings.ToLower(path.Ext(upload.OriginalName))
	if !extension.MatchString(ext) {
		ext = ""
	}
	return upload.WorkspaceID + "/" + upload.MediaID + "/original" + ext
}

func (a *API) upload(w http.ResponseWriter, r *http.Request) {
	mediaID := r.PathValue("id")
	if !ids.Valid(mediaID, "med") {
		writeError(w, http.StatusNotFound, "not_found", "There is no such upload.")
		return
	}
	claims, err := a.signer.VerifyUpload(mediaID, r.URL.Query().Get("token"))
	if err != nil {
		linkError(w, err)
		return
	}

	// Receive into a file first: the size and the checksum are known before anything is stored.
	limit := a.cfg.MaxUploadMB << 20
	spool, err := os.CreateTemp(a.cfg.WorkDir, "likho-upload-")
	if err != nil {
		a.internal(w, "could not create a temporary file", err)
		return
	}
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spool.Name())
	}()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(spool, hash), http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "too_large", "The file is larger than the upload limit.")
			return
		}
		writeError(w, http.StatusBadRequest, "upload_interrupted", "The upload stopped before the whole file arrived.")
		return
	}
	if size == 0 {
		writeError(w, http.StatusBadRequest, "empty_file", "The file is empty.")
		return
	}
	if err := spool.Close(); err != nil {
		a.internal(w, "could not write the temporary file", err)
		return
	}
	sum := hex.EncodeToString(hash.Sum(nil))

	// The file is here. Storing it must not wait for ever on a store that does not answer.
	ctx, cancel := context.WithTimeout(r.Context(), storeTimeout)
	defer cancel()

	// The same link used twice: the same file is fine, a different one is not.
	if existing, err := a.store.Get(ctx, mediaID); err == nil {
		a.answerExisting(w, existing, sum)
		return
	} else if !errors.Is(err, store.ErrNotFound) {
		a.internal(w, "could not read the media table", err)
		return
	}

	contentType := claims.ContentType
	if contentType == "" {
		contentType = r.Header.Get("Content-Type")
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	key := objectKey(claims)
	if err := a.objects.PutFile(ctx, a.cfg.BucketOriginal, key, spool.Name(), contentType); err != nil {
		a.internal(w, "could not store the file", err)
		return
	}
	stored, err := a.store.Insert(ctx, store.Media{
		ID: mediaID, WorkspaceID: claims.WorkspaceID, RecordingID: claims.RecordingID,
		OriginalName: claims.OriginalName, ContentType: contentType, SizeBytes: size, SHA256: sum, OriginalKey: key,
	})
	if errors.Is(err, store.ErrExists) { // two uploads to one link at the same moment
		if existing, getErr := a.store.Get(ctx, mediaID); getErr == nil {
			a.answerExisting(w, existing, sum)
			return
		}
	}
	if err != nil {
		a.internal(w, "could not record the file", err)
		return
	}

	result := UploadResult{MediaID: stored.ID, SHA256: sum, SizeBytes: size, Status: stored.Status}
	if earlier, err := a.store.FindBySHA256(ctx, stored.WorkspaceID, sum, stored.ID); err == nil {
		result.DuplicateOf = earlier.ID
	}
	a.wake()
	a.log.Info("uploaded", "media_id", stored.ID, "size_bytes", size)
	writeJSON(w, http.StatusCreated, result)
}

func (a *API) answerExisting(w http.ResponseWriter, existing store.Media, sum string) {
	if existing.SHA256 != sum {
		writeError(w, http.StatusConflict, "already_uploaded", "A different file was already uploaded with this link.")
		return
	}
	writeJSON(w, http.StatusOK, UploadResult{
		MediaID: existing.ID, SHA256: existing.SHA256, SizeBytes: existing.SizeBytes, Status: existing.Status,
	})
}

func (a *API) internal(w http.ResponseWriter, what string, err error) {
	a.log.Error(what, "error", err)
	writeError(w, http.StatusInternalServerError, "internal", "Something went wrong on our side. Try again.")
}

func (a *API) download(w http.ResponseWriter, r *http.Request) {
	mediaID, kind := r.PathValue("id"), r.PathValue("kind")
	if !ids.Valid(mediaID, "med") {
		writeError(w, http.StatusNotFound, "not_found", "There is no such file.")
		return
	}
	if err := a.signer.VerifyDownload(mediaID, kind, r.URL.Query()); err != nil {
		linkError(w, err)
		return
	}
	media, err := a.store.Get(r.Context(), mediaID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "There is no such file.")
		return
	}
	if err != nil {
		a.internal(w, "could not read the media table", err)
		return
	}

	var bucket, key, contentType string
	switch kind {
	case links.KindOriginal:
		bucket, key = a.cfg.BucketOriginal, media.OriginalKey
	case links.KindAudio:
		if media.Status == store.StatusReady {
			// What a browser plays: the copy made for it, or the original when that plays as it is.
			bucket, key, contentType = a.cfg.BucketPlayback, media.PlaybackKey, media.PlaybackContentType
			if key == "" {
				bucket, key = a.cfg.BucketOriginal, media.OriginalKey
			}
		}
	case links.KindPeaks:
		bucket, key = a.cfg.BucketPeaks, media.PeaksKey
	}
	if key == "" {
		writeError(w, http.StatusConflict, "not_ready", "The file has not been converted yet.")
		return
	}
	object, err := a.objects.Open(r.Context(), bucket, key)
	if errors.Is(err, objects.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "There is no such file.")
		return
	}
	if err != nil {
		a.internal(w, "could not open the object", err)
		return
	}
	defer func() { _ = object.Close() }()

	if contentType == "" {
		contentType = object.ContentType
	}
	w.Header().Set("Content-Type", contentType)
	if object.ETag != "" {
		w.Header().Set("ETag", `"`+object.ETag+`"`)
	}
	// The content behind a media id never changes, so the browser may keep it while it plays.
	w.Header().Set("Cache-Control", "private, max-age=3600")
	// ServeContent answers range requests, which is what lets the player jump to a line.
	http.ServeContent(w, r, "", time.Time{}, object)
}

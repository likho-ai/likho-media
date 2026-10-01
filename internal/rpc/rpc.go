// Package rpc implements likho.media.v1.MediaService, the interface the other services call.
package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"connectrpc.com/connect"
	mediav1 "github.com/likho-ai/likho-contracts/packages/go/gen/likho/media/v1"
	"github.com/likho-ai/likho-contracts/packages/go/gen/likho/media/v1/mediav1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/likho-ai/likho-media/internal/config"
	"github.com/likho-ai/likho-media/internal/ids"
	"github.com/likho-ai/likho-media/internal/links"
	"github.com/likho-ai/likho-media/internal/objects"
	"github.com/likho-ai/likho-media/internal/store"
)

// Service implements the MediaService handler.
type Service struct {
	mediav1connect.UnimplementedMediaServiceHandler

	cfg     config.Config
	store   *store.Store
	objects *objects.Store
	signer  *links.Signer
	log     *slog.Logger
}

// New returns the service.
func New(cfg config.Config, db *store.Store, objs *objects.Store, signer *links.Signer, log *slog.Logger) *Service {
	return &Service{cfg: cfg, store: db, objects: objs, signer: signer, log: log}
}

var statuses = map[string]mediav1.MediaStatus{
	store.StatusUploaded: mediav1.MediaStatus_MEDIA_STATUS_UPLOADED,
	store.StatusReady:    mediav1.MediaStatus_MEDIA_STATUS_READY,
	store.StatusFailed:   mediav1.MediaStatus_MEDIA_STATUS_FAILED,
}

func toProto(m store.Media) *mediav1.Media {
	return &mediav1.Media{
		Id:              m.ID,
		OriginalName:    m.OriginalName,
		Sha256:          m.SHA256,
		ContentType:     m.ContentType,
		SizeBytes:       uint64(max(m.SizeBytes, 0)),
		DurationSeconds: m.DurationSeconds,
		Channels:        uint32(max(m.Channels, 0)),   //nolint:gosec // never negative
		SampleRate:      uint32(max(m.SampleRate, 0)), //nolint:gosec // never negative
		Status:          statuses[m.Status],
		FailureReason:   m.FailureReason,
		CreatedAt:       timestamppb.New(m.CreatedAt),
		WorkspaceId:     m.WorkspaceID,
		RecordingId:     m.RecordingID,
	}
}

func invalid(format string, args ...any) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(format, args...))
}

func (s *Service) internal(what string, err error) error {
	s.log.Error(what, "error", err)
	return connect.NewError(connect.CodeInternal, errors.New("the media service could not complete the request"))
}

func (s *Service) get(ctx context.Context, id string) (store.Media, error) {
	if id == "" {
		return store.Media{}, invalid("id is required")
	}
	media, err := s.store.Get(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return store.Media{}, connect.NewError(connect.CodeNotFound, fmt.Errorf("media %q not found", id))
	}
	if err != nil {
		return store.Media{}, s.internal("could not read the media table", err)
	}
	return media, nil
}

// GetMedia returns what is known about one file.
func (s *Service) GetMedia(ctx context.Context, req *connect.Request[mediav1.GetMediaRequest]) (*connect.Response[mediav1.GetMediaResponse], error) {
	media, err := s.get(ctx, req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&mediav1.GetMediaResponse{Media: toProto(media)}), nil
}

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// CreateUpload returns a link to send one file to, unless the same content is already stored.
func (s *Service) CreateUpload(ctx context.Context, req *connect.Request[mediav1.CreateUploadRequest]) (*connect.Response[mediav1.CreateUploadResponse], error) {
	msg := req.Msg
	switch {
	case !ids.Valid(msg.GetWorkspaceId(), "wsp"):
		return nil, invalid("workspace_id is required and must be a workspace id")
	case !ids.Valid(msg.GetRecordingId(), "rec"):
		return nil, invalid("recording_id is required and must be a recording id")
	case strings.TrimSpace(msg.GetOriginalName()) == "":
		return nil, invalid("original_name is required")
	case len(msg.GetOriginalName()) > 255:
		return nil, invalid("original_name is longer than 255 characters")
	case msg.GetSha256() != "" && !sha256Pattern.MatchString(msg.GetSha256()):
		return nil, invalid("sha256 must be 64 lowercase hex characters")
	case msg.GetSizeBytes() > uint64(s.cfg.MaxUploadMB)<<20: //nolint:gosec // a small positive setting
		return nil, invalid("the file is larger than the upload limit of %d MB", s.cfg.MaxUploadMB)
	}

	if msg.GetSha256() != "" {
		existing, err := s.store.FindBySHA256(ctx, msg.GetWorkspaceId(), msg.GetSha256(), "")
		if err == nil {
			return connect.NewResponse(&mediav1.CreateUploadResponse{MediaId: existing.ID, ExistingMedia: toProto(existing)}), nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return nil, s.internal("could not read the media table", err)
		}
	}

	mediaID := ids.New("med")
	url, expires := s.signer.UploadURL(links.Upload{
		MediaID:      mediaID,
		WorkspaceID:  msg.GetWorkspaceId(),
		RecordingID:  msg.GetRecordingId(),
		OriginalName: strings.TrimSpace(msg.GetOriginalName()),
		ContentType:  msg.GetContentType(),
	}, s.cfg.UploadTTL)
	return connect.NewResponse(&mediav1.CreateUploadResponse{
		MediaId: mediaID, UploadUrl: url, ExpiresAt: timestamppb.New(expires),
	}), nil
}

// GetDownloadUrl returns a short-lived link to the original, the audio a browser plays, or the waveform.
func (s *Service) GetDownloadUrl(ctx context.Context, req *connect.Request[mediav1.GetDownloadUrlRequest]) (*connect.Response[mediav1.GetDownloadUrlResponse], error) {
	var kind string
	switch req.Msg.GetKind() {
	case mediav1.MediaKind_MEDIA_KIND_ORIGINAL:
		kind = links.KindOriginal
	case mediav1.MediaKind_MEDIA_KIND_NORMALIZED:
		kind = links.KindAudio
	case mediav1.MediaKind_MEDIA_KIND_PEAKS:
		kind = links.KindPeaks
	default:
		return nil, invalid("kind is required")
	}
	media, err := s.get(ctx, req.Msg.GetId())
	if err != nil {
		return nil, err
	}
	if kind != links.KindOriginal && media.Status != store.StatusReady {
		reason := "the file has not been converted yet"
		if media.Status == store.StatusFailed {
			reason = "the file could not be converted: " + media.FailureReason
		}
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(reason))
	}
	url, expires := s.signer.DownloadURL(media.ID, kind, s.cfg.DownloadTTL)
	return connect.NewResponse(&mediav1.GetDownloadUrlResponse{Url: url, ExpiresAt: timestamppb.New(expires)}), nil
}

// DeleteMedia removes a file and everything made from it. Deleting twice is not an error.
func (s *Service) DeleteMedia(ctx context.Context, req *connect.Request[mediav1.DeleteMediaRequest]) (*connect.Response[mediav1.DeleteMediaResponse], error) {
	if req.Msg.GetId() == "" {
		return nil, invalid("id is required")
	}
	media, err := s.store.Delete(ctx, req.Msg.GetId())
	if errors.Is(err, store.ErrNotFound) {
		return connect.NewResponse(&mediav1.DeleteMediaResponse{}), nil
	}
	if err != nil {
		return nil, s.internal("could not delete the media row", err)
	}
	// The row is gone, so nothing points at the objects any more. An object that cannot be
	// removed now is only wasted space, which is why this is logged and not returned.
	for bucket, key := range map[string]string{
		s.cfg.BucketOriginal: media.OriginalKey, s.cfg.BucketPlayback: media.PlaybackKey, s.cfg.BucketPeaks: media.PeaksKey,
	} {
		if err := s.objects.Remove(ctx, bucket, key); err != nil {
			s.log.Warn("could not remove an object of a deleted file", "media_id", media.ID, "bucket", bucket, "error", err)
		}
	}
	s.log.Info("deleted", "media_id", media.ID)
	return connect.NewResponse(&mediav1.DeleteMediaResponse{}), nil
}

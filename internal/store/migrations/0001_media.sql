-- One row per uploaded file. A row appears when the bytes have arrived, never before.
CREATE TABLE media (
    id                    text PRIMARY KEY,
    workspace_id          text NOT NULL,
    recording_id          text NOT NULL,
    original_name         text NOT NULL,
    content_type          text NOT NULL DEFAULT '',
    size_bytes            bigint NOT NULL,
    sha256                text NOT NULL,
    status                text NOT NULL CHECK (status IN ('uploaded', 'ready', 'failed')),
    failure_code          text NOT NULL DEFAULT '',
    failure_reason        text NOT NULL DEFAULT '',
    original_key          text NOT NULL,
    -- A copy browsers can play. Empty when the original is one already.
    playback_key          text NOT NULL DEFAULT '',
    -- The content type browsers play the audio as (of the copy, or of the original).
    playback_content_type text NOT NULL DEFAULT '',
    peaks_key             text NOT NULL DEFAULT '',
    duration_seconds      double precision NOT NULL DEFAULT 0,
    channels              integer NOT NULL DEFAULT 0,
    sample_rate           integer NOT NULL DEFAULT 0,
    codec                 text NOT NULL DEFAULT '',
    -- Conversion: how often it was tried, and who is working on it since when.
    attempts              integer NOT NULL DEFAULT 0,
    claimed_at            timestamptz,
    -- When each event was published. NULL means it still has to go out.
    uploaded_published_at timestamptz,
    outcome_published_at  timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

-- "Was this content uploaded to this workspace before?"
CREATE INDEX media_workspace_sha256 ON media (workspace_id, sha256, created_at);
-- The conversion queue.
CREATE INDEX media_waiting ON media (created_at) WHERE status = 'uploaded';
-- Events that still have to be published.
CREATE INDEX media_unpublished ON media (updated_at) WHERE status <> 'uploaded' AND outcome_published_at IS NULL;

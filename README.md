# likho-media

The media service of [Likho](https://github.com/likho-ai). It is the only door to the audio:
recordings come in through it, and everything that needs audio gets a short-lived link from it.

| It does | How |
| --- | --- |
| Takes a recording | `CreateUpload` returns a link; the file is sent to it with `PUT` |
| Checks that it is audio | FFmpeg reads it: duration, channels, sample rate, codec |
| Makes the waveform | Peaks as JSON, 20 per second, for the player |
| Makes it playable | Only when browsers cannot play the file as it is |
| Hands out audio | `GetDownloadUrl` returns a signed link; range requests work |
| Tells the others | `likho.media.uploaded`, then `likho.media.ready` or `likho.media.failed` |

Go, [Connect](https://connectrpc.com) (which also answers plain gRPC), PostgreSQL, an S3 object
store, NATS JetStream, FFmpeg.

## The original is never changed

The speech model reads a recording **exactly as it was uploaded**. This is on purpose. On real
calls, transcribing a converted 16 kHz copy instead of the original changed about one word in
seven, and a lossless copy was twelve times the size of a telephone MP3.

So the only copy this service makes is for playing:

| The file is | A browser gets |
| --- | --- |
| MP3, AAC in MP4, FLAC, or WAV with plain PCM | The original itself. Nothing is stored twice. |
| Anything else FFmpeg reads: A-law, mu-law, GSM, AMR, WMA, Opus, ... | An MP3 copy, made once |

## How a file travels

```
likho-api                         likho-media                      object store / bus
   |  CreateUpload(workspace, recording, name)  |
   |------------------------------------------->|
   |  media_id + upload link (valid 1 hour)     |
   |<-------------------------------------------|
browser or connector                            |
   |  PUT link  (the file)                      |  sha256 and size while it arrives
   |------------------------------------------->|-----------------> likho-audio/<workspace>/<media>/original.mp3
   |  201 {media_id, sha256, duplicate_of?}     |  row: uploaded
   |<-------------------------------------------|-----------------> likho.media.uploaded
                                                |  a worker: probe, waveform, playback copy if needed
                                                |-----------------> likho-peaks/.../peaks.json
                                                |  row: ready      -> likho.media.ready
                                                |  (not audio: row failed -> likho.media.failed)
```

What you can rely on:

- **Nothing is stored for a link that was never used.** The upload link carries what it stands
  for, signed. A row appears when the bytes have arrived.
- **The same link twice** gives the same answer for the same file, and `409` for a different one.
- **The same content again** is stored, and the answer says which earlier file it repeats
  (`duplicate_of`). A caller that knows the checksum passes it to `CreateUpload` and is spared
  the upload.
- **A file is not lost when the service dies.** The queue is the table: a worker claims the oldest
  waiting file, and a file whose worker died is claimed again by another one. Several instances
  share the work without a coordinator.
- **An event is not lost when the bus is down.** The result is stored first; the event is sent
  until the bus confirms it. Event ids are derived from the media id, so a repeated event is
  stored once.
- **Failures are sorted.** A file that is not audio fails at once, with a reason in plain words.
  A dependency that is down means another attempt, 5 in all, each waiting longer.
- **Stopping is clean.** On SIGTERM, requests in flight and files being worked on are finished.

## Links

The object store is never reachable from outside. A link points at this service, is valid for one
thing, and expires (uploads after 1 hour, downloads after 15 minutes). The signature is an
HMAC-SHA256 with `LINK_SECRET`.

| Link | What it does |
| --- | --- |
| `PUT /media/uploads/{media_id}?token=...` | Receives the file. `201` with `media_id`, `sha256`, `size_bytes`, `status`, `duplicate_of` |
| `GET /media/{media_id}/original?...` | The file as it was uploaded. This is what likho-transcription downloads |
| `GET /media/{media_id}/audio?...` | What a browser plays. `Range` requests are answered, so the player can jump to a line |
| `GET /media/{media_id}/peaks?...` | The waveform |

Errors have the shape every Likho HTTP API uses:
`{"error": {"code": "link_expired", "message": "This link has expired. Ask for a new one."}}`.
Codes: `link_invalid`, `link_expired` (403), `too_large` (413), `empty_file` (400),
`already_uploaded` (409), `not_ready` (409), `not_found` (404).

The waveform:

```json
{ "version": 1, "duration_seconds": 61.2, "peaks_per_second": 20, "max": 100, "peaks": [0, 3, 41, 87, 52] }
```

## gRPC

`likho.media.v1.MediaService` on port 5010, defined in
[likho-contracts](https://github.com/likho-ai/likho-contracts). The standard gRPC health service
is served on the same port.

| Call | What it does |
| --- | --- |
| `CreateUpload` | A link to send one file to. With a known `sha256` of stored content: that file, and no link |
| `GetMedia` | Status, size, checksum, duration, channels, sample rate, failure reason |
| `GetDownloadUrl` | A link to the original, the playable audio (`NORMALIZED`) or the waveform (`PEAKS`) |
| `DeleteMedia` | Removes the file and everything made from it. Deleting twice is not an error |

## Run it

Needs PostgreSQL, NATS and the object store from the
[likho-infra](https://github.com/likho-ai/likho-infra) stack (`bash scripts/up.sh`), and FFmpeg on
the PATH.

```bash
go run ./cmd/likho-media      # HTTP on 4010, gRPC on 5010; creates its table on start
```

With Docker, on the stack's network (FFmpeg is in the image):

```bash
docker build -t likho-media .
docker run --rm --network likho -p 4010:4010 -p 5010:5010 \
  -e DATABASE_URL=postgres://likho_media:likho_media@postgres:5432/likho_media \
  -e NATS_URL=nats://nats:4222 -e S3_ENDPOINT=http://objectstore:8333 \
  likho-media
```

## Configuration

Settings come from environment variables and from `.env` files chosen by `LIKHO_ENV`
(`development` by default). The files are read in this order, each overriding the one before,
and a real environment variable wins over all of them:

```
.env   .env.local   .env.<LIKHO_ENV>   .env.<LIKHO_ENV>.local
```

`.env.development`, `.env.staging` and `.env.production` are committed and hold no secrets.
`.env.<env>.local` holds the secrets of that environment on your machine; git ignores it, and
`likho-infra/scripts/make-env-secrets.py` makes it. In Kubernetes the same values come from
ConfigMaps and Secrets.

| Variable | Default | Meaning |
| --- | --- | --- |
| `HTTP_PORT` | `4010` | Uploads and downloads, `GET /healthz` (alive), `GET /readyz` (database and bus answer) |
| `GRPC_PORT` | `5010` | `MediaService` and gRPC health |
| `DATABASE_URL` | local stack, database `likho_media` | PostgreSQL |
| `NATS_URL` | `nats://localhost:4222` | Event bus |
| `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` | local stack | Object store |
| `S3_BUCKET_ORIGINAL`, `S3_BUCKET_PLAYBACK`, `S3_BUCKET_PEAKS` | `likho-audio`, `likho-normalized`, `likho-peaks` | One bucket per kind of object |
| `PUBLIC_URL` | `http://localhost:8080` | The address links start with: the gateway |
| `INTERNAL_URL` | empty (= `PUBLIC_URL`) | The address other services reach this service at; links to the original start with it. In a cluster: `http://likho-media:4010` |
| `LINK_SECRET` | a development value | Signs links. The service refuses to start in production without its own |
| `UPLOAD_TTL_SECONDS` / `DOWNLOAD_TTL_SECONDS` | `3600` / `900` | How long links work |
| `MAX_UPLOAD_MB` | `500` | Larger files are refused |
| `WORKERS` | `2` | Files worked on at the same time |
| `MAX_ATTEMPTS` | `5` | Attempts when a dependency fails |
| `LOG_LEVEL` | `INFO` | Logs are JSON, one object per line |

## Develop

```bash
go vet ./...
golangci-lint run          # v2
go test ./...
```

The tests run the real service against the local stack and generate their own audio with FFmpeg
(tones, in the formats telephone systems produce). Each test gets its own database schema.
Without the stack they are skipped; with `LIKHO_REQUIRE_STACK=1` (set in CI) they fail instead.

No recording belongs in this repository.

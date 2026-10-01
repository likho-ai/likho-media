# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/likho-media ./cmd/likho-media

FROM alpine:3.24
# ffmpeg and ffprobe do the reading and converting of audio.
RUN apk add --no-cache ffmpeg \
    && addgroup -S likho && adduser -S -u 10001 -G likho likho
COPY --from=build /out/likho-media /usr/local/bin/likho-media
USER likho
ENV WORK_DIR=/tmp
EXPOSE 4010 5010
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=5 \
    CMD ["wget", "-qO-", "http://127.0.0.1:4010/readyz"]
CMD ["likho-media"]

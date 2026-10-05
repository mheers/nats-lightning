# syntax=docker/dockerfile:1

# Build stage. The dependency layer is copied on its own so that editing source
# does not re-download the module graph on every build.
FROM golang:1.26-alpine AS build

# git is needed by go mod download for modules resolved from VCS, and the
# toolchain lines below need nothing else.
RUN apk add --no-cache git

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/

# A static binary, because SQLite here is the pure-Go modernc.org/sqlite driver
# rather than a cgo binding: the container needs no C toolchain at runtime, and
# the same build runs on any architecture without a cross-compiler.
#
# Both binaries land in one image on purpose. The demo is how somebody checks
# that a consumer can actually read the stream, and an image that could only run
# the bridge would make that a local build.
ARG VERSION=dev
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/lightningfeed ./cmd/lightningfeed \
 && go build -trimpath -ldflags "-s -w" -o /out/lightningfeed-demo ./cmd/lightningfeed-demo


# Runtime stage.
FROM alpine:3.22

# ca-certificates is not optional: the upstream is wss://, so without the trust
# store the TLS handshake fails and the bridge reconnects to a server that will
# never accept it. The failure looks like a network problem rather than a missing
# package, which is why this is worth its own layer.
RUN apk add --no-cache ca-certificates tzdata

# A dedicated unprivileged account. The process needs no privileges at all: it
# writes to one directory and opens two sockets.
RUN addgroup -g 10001 -S lightningfeed \
 && adduser -u 10001 -S -G lightningfeed -h /nonexistent -s /sbin/nologin lightningfeed \
 && mkdir -p /var/lib/lightningfeed \
 && chown lightningfeed:lightningfeed /var/lib/lightningfeed

COPY --from=build /out/lightningfeed /out/lightningfeed-demo /usr/local/bin/

USER lightningfeed:lightningfeed
WORKDIR /var/lib/lightningfeed
VOLUME /var/lib/lightningfeed

EXPOSE 9109

# The probe is /metrics rather than /healthz, deliberately.
#
# /healthz reports not-ready for a standby, which is correct behaviour and not a
# fault: a standby is working exactly as designed and should not be sent traffic.
# Using it as a container healthcheck would therefore mark a perfectly healthy
# standby unhealthy. /metrics answers 200 whenever the process is up and serving,
# which is the question a container healthcheck is actually asking.
#
# 9109 is the default; a deployment that moves it must move this too.
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:9109/metrics || exit 1

# ENTRYPOINT is the bridge and CMD is its subcommand, so `docker run
# nats-lightning --version` and `docker run nats-lightning history --since=1h`
# both do the obvious thing.
#
# It does not affect the demo: docker exec and docker compose exec run their
# argument directly and ignore ENTRYPOINT, so `docker compose exec lightningfeed
# lightningfeed-demo` still works.
ENTRYPOINT ["lightningfeed"]
CMD ["ingest"]
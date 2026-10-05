# Build, test, package and demonstrate nats-lightning.
#
#   make            list the targets
#   make demo       start everything and watch real lightning arrive
#   make docker-push publish the image to Docker Hub
#
# Nothing here is required to build the project — `go build ./...` is still the
# whole of it. These targets exist so the common operations are one word, and so
# that the demo does not depend on somebody remembering the flags.

SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# Where the image is published.
IMAGE ?= mheers/nats-lightning

# The version is the last tag, so a release is "git tag && make docker-push" with
# nothing to edit. A repository with no tags yet yields the short commit, which is
# still a usable image tag.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

# A release is immutable, so `latest` is applied by its own target rather than
# silently on every push: overwriting it from a branch would leave the tag pointing
# at something nobody tested.
TAGS ?= $(VERSION)

GO ?= go
COMPOSE ?= docker compose
DOCKER ?= docker

# Where locally built binaries land, and what the host platform is for image tags.
BIN := bin
GOOS ?= $(shell $(GO) env GOOS)
GOARCH ?= $(shell $(GO) env GOARCH)

# Architectures for the multi-platform push. Go produces a static binary and this
# image has no cgo, so cross-building is a compiler flag rather than a toolchain,
# and an arm64 machine — most of them now — gets a first-class image.
PLATFORMS ?= linux/amd64,linux/arm64

# The demo's own settings.
#
# World-wide on purpose, and this is the part worth understanding rather than
# copying. A regional demo depends on there being a storm over that circle right
# now, which is false most of the time: the region in .env.example is a quiet 10 km
# circle in Crete that can go hours without a stroke, and a demo that waits in
# silence looks exactly like a broken install. World-wide there is always lightning
# somewhere — measured at roughly 20 strokes a second — so the demo shows something
# the moment it starts.
DURATION ?= 60s

# A region, for a focused demo. All three or none; see the demo target.
NAME ?=
LAT ?=
LON ?=
RADIUS_KM ?=

.PHONY: help
help: ## List the available targets
	@printf 'nats-lightning %s\n\n' '$(VERSION)'
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-17s\033[0m %s\n", $$1, $$2}'
	@printf '\nImage %s, platform %s/%s\n' '$(IMAGE)' '$(GOOS)' '$(GOARCH)'
	@printf '\nA region for the demo, all three or none:\n'
	@printf '  make demo NAME=crete LAT=35.3340688 LON=24.4944483 RADIUS_KM=25\n'
	@printf '\nThe demo is world-wide by default: a regional demo shows nothing unless there is\n'
	@printf 'a storm over it, and a demo that waits in silence looks like a broken install.\n\n'

# --- build and test ---------------------------------------------------------

.PHONY: build
build: ## Build both binaries into ./bin
	@mkdir -p $(BIN)
	$(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN)/lightningfeed ./cmd/lightningfeed
	$(GO) build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $(BIN)/lightningfeed-demo ./cmd/lightningfeed-demo
	@ls -l $(BIN)

.PHONY: test
test: ## Run the unit and integration tests
	$(GO) test ./...

.PHONY: test-race
test-race: ## Run the tests under the race detector
	$(GO) test -race ./...

.PHONY: lint
lint: ## Check formatting and run go vet
	@unformatted=$$(gofmt -l .); \
	if [ -n "$$unformatted" ]; then \
		echo "these files are not gofmt'd:"; echo "$$unformatted"; exit 1; \
	fi
	$(GO) vet ./...

.PHONY: ci
ci: lint test-race build ## Everything the CI workflow runs

# --- image ------------------------------------------------------------------

.PHONY: docker-build
docker-build: ## Build the container image for this machine
	$(COMPOSE) build

.PHONY: docker-push
docker-push: ## Push this machine's architecture to Docker Hub (docker login first)
	$(DOCKER) buildx build --platform $(GOOS)/$(GOARCH) \
		$(foreach t,$(TAGS),-t $(IMAGE):$(t)) --push .

.PHONY: docker-push-latest
docker-push-latest: ## Also move the mutable `latest` tag
	$(MAKE) --no-print-directory docker-push TAGS="$(TAGS) latest"
	@echo "note: 'latest' is now $(IMAGE):$(VERSION)"

.PHONY: docker-pushx
docker-pushx: ## Push a multi-architecture image (needs a buildx builder)
	$(DOCKER) buildx build --platform $(PLATFORMS) \
		$(foreach t,$(TAGS),-t $(IMAGE):$(t)) --push .

.PHONY: docker-run
docker-run: ## Run the published image, publishing the metrics port
	$(DOCKER) run --rm -it \
		-p 127.0.0.1:9109:9109 \
		-v lightningfeed-demo-data:/var/lib/lightningfeed \
		$(IMAGE):$(VERSION)

# --- the stack --------------------------------------------------------------

.env:
	@cp .env.example .env
	@echo "wrote .env from .env.example — set your region in it to narrow the feed"

.PHONY: up
up: .env ## Start the broker and the bridge, and wait for a live upstream
	@# --build, because a demo that shows yesterday's binary is worse than no demo.
	@# The layer cache makes it near-instant when nothing changed, and a Go build in
	@# a container is a few seconds when something did.
	$(COMPOSE) up -d --wait --build
	@$(MAKE) --no-print-directory wait-upstream

# Starting the bridge is not the same as it working: it binds its metrics port
# before it dials, so compose reporting healthy says nothing about whether any data
# is flowing. The upstream throttles new connections — measured 0 of 6 succeeding
# below a 15 second gap — so a refusal here is often transient, which is why this
# waits and explains rather than failing on the first attempt.
.PHONY: wait-upstream
wait-upstream:
	@for i in $$(seq 1 30); do \
		if $(COMPOSE) exec -T lightningfeed wget -qO- http://127.0.0.1:9109/metrics 2>/dev/null \
			| grep -q '^lightningfeed_upstream_connected 1'; then \
			echo "upstream connected"; exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "no live upstream connection after 60s."; \
	echo "The bridge serves its metrics before it dials, so this is not a crash."; \
	echo "The upstream throttles new connections, so a refusal is often transient."; \
	echo "Check: $(COMPOSE) logs lightningfeed | tail -20"; \
	exit 1

.PHONY: down
down: ## Stop the stack and delete its volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Follow the bridge's logs
	$(COMPOSE) logs -f lightningfeed

.PHONY: ps
ps: ## Show what is running
	$(COMPOSE) ps

.PHONY: metrics
metrics: ## Print the bridge's metrics once
	$(COMPOSE) exec -T lightningfeed wget -qO- http://127.0.0.1:9109/metrics

.PHONY: subjects
subjects: ## Print the subjects this region publishes to
	$(COMPOSE) exec -T lightningfeed lightningfeed cells

# --- the demo ---------------------------------------------------------------

# demo starts the broker and the bridge, waits for a live upstream, and draws what
# arrives.
#
# Both processes read the same .env, which is what makes this reliable: the bridge
# publishes what its region covers and the demo subscribes to exactly that. Pointing
# them at different regions leaves the demo watching an empty stream, and the
# symptom — silence — is indistinguishable from a broken install.
#
# So the region is set in one place. Given on the command line it is written into
# .env and the bridge restarted, which is a visible change to the configuration
# rather than a hidden override:
#
#   make demo NAME=crete LAT=35.3340688 LON=24.4944483 RADIUS_KM=25 DURATION=120s
#
# A regional demo shows nothing unless there is a storm over it right now. The
# default is world-wide because that is reliably busy — measured at roughly 20
# strokes a second — so a fresh clone produces a working demo.
.PHONY: demo
demo: .env ## Start the stack and watch live lightning arrive
	@$(MAKE) --no-print-directory demo-internal VIEW=map

.PHONY: demo-log
demo-log: .env ## Watch the demo as log lines, for piping or grepping
	@$(MAKE) --no-print-directory demo-internal VIEW=log TTY=-T

# demo-internal does the work for both, so that the region handling, the durable
# name and the wait cannot drift apart between them.
.PHONY: demo-internal
demo-internal: .env
	@if [ -n "$(LAT)" ] || [ -n "$(LON)" ] || [ -n "$(RADIUS_KM)" ]; then \
		if [ -z "$(LAT)" ] || [ -z "$(LON)" ] || [ -z "$(RADIUS_KM)" ]; then \
			echo "a region needs all three of LAT, LON and RADIUS_KM"; exit 1; \
		fi; \
		echo "setting the region in .env: $(or $(NAME),demo) $(LAT),$(LON) $(RADIUS_KM)km"; \
		$(SET_REGION); \
		durable='make-demo-$(or $(NAME),region)'; \
	else \
		durable='make-demo-world'; \
	fi; \
	$(MAKE) --no-print-directory up; \
	$(COMPOSE) run --rm $(TTY) --entrypoint lightningfeed-demo demo \
		--view '$(VIEW)' --duration '$(DURATION)' --durable "$$durable"; \
	if [ '$(VIEW)' = 'map' ]; then \
		echo; \
		echo "The archive grows with the feed: world-wide is roughly 20 strokes a"; \
		echo "second at about 1.5 kB each, so a few GB a day. 'make down' deletes it."; \
	fi

# SET_REGION writes a region into .env.
#
# Sed rather than a regenerated file, so everything else in .env survives — the
# retention, the log level, anything a reader has added. An empty value is how this
# project spells "not set", so blanking the line is what returns to world-wide, and
# the pattern matches the commented form as well as an active one.
define SET_REGION
sed -i -e 's|^#* *LIGHTNINGFEED_REGION_NAME=.*|LIGHTNINGFEED_REGION_NAME=$(or $(NAME),demo)|' \
       -e 's|^#* *LIGHTNINGFEED_REGION_LAT=.*|LIGHTNINGFEED_REGION_LAT=$(LAT)|' \
       -e 's|^#* *LIGHTNINGFEED_REGION_LON=.*|LIGHTNINGFEED_REGION_LON=$(LON)|' \
       -e 's|^#* *LIGHTNINGFEED_REGION_RADIUS_KM=.*|LIGHTNINGFEED_REGION_RADIUS_KM=$(RADIUS_KM)|' .env
endef

# --- housekeeping -----------------------------------------------------------

.PHONY: clean
clean: ## Remove local build output and stray databases
	rm -rf $(BIN)
	rm -f lightningfeed lightningfeed-demo
	rm -f ./*.db ./*.db-wal ./*.db-shm

.PHONY: distclean
distclean: clean down ## Also remove the containers and volumes
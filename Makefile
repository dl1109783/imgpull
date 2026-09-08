BINARY := imgpull
BIN_DIR := bin
PREFIX ?= /usr/local

# Fall back to the common install location when go is not on PATH.
GO := $(shell command -v go 2>/dev/null || echo /usr/local/go/bin/go)

.PHONY: all build install test test-short fmt vet clean

all: build

build:
	$(GO) build -trimpath -ldflags "-s -w" -o $(BIN_DIR)/$(BINARY) ./cmd/imgpull

install: build
	install -m 0755 $(BIN_DIR)/$(BINARY) $(PREFIX)/bin/$(BINARY)

test: vet
	$(GO) test ./... -count=1

# Skip tests that spawn a real aria2c daemon (aria2c not installed / CI).
test-short: vet
	$(GO) test -short ./... -count=1

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

clean:
	rm -rf $(BIN_DIR)

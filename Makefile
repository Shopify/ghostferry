# Variables to be built into the binary
VERSION         := 1.1.0

# This variable can be overwritten by the caller
DATETIME        ?= $(shell date -u +%Y%m%d%H%M%S)

ifndef IGNORE_DIRTY_TREE
DIRTY_TREE      := $(shell git diff-index --quiet HEAD -- || echo '+dirty')
endif

COMMIT_SHA      ?= $(shell git rev-parse --short HEAD)
COMMIT          := $(addsuffix $(DIRTY_TREE),$(COMMIT_SHA))
VERSION_STR     := $(VERSION)+$(DATETIME)+$(COMMIT)

# Flags
LDFLAGS         := -X github.com/Shopify/ghostferry.VersionString=$(VERSION_STR)

# Paths
FIRSTGOPATH     := $(firstword $(subst :, ,$(GOPATH)))
GOBIN           := $(FIRSTGOPATH)/bin
BUILD_DIR       := build
DEB_PREFIX      := $(BUILD_DIR)/debian
SHARE_DIR       := usr/share/ghostferry
BIN_DIR         := usr/bin

# Targets
PROJECTS        := copydb sharding
PROJECT_DEBS    := $(foreach name,$(PROJECTS),$(name)-deb)

PLATFORM        := $(shell uname -s | tr A-Z a-z)
ARCH            := $(shell uname -m | sed 's/x86_64/amd64/g' | sed 's/aarch64/arm64/g')
GOTESTSUM_URL   := "https://github.com/gotestyourself/gotestsum/releases/download/v1.7.0/gotestsum_1.7.0_$(PLATFORM)_$(ARCH).tar.gz"

# Target specific variable, set proj to have a valid value.
PROJECT_PKG      = ./$(proj)/cmd
PROJECT_BIN      = ghostferry-$(proj)$(PROJECT_BIN_TAG)
BIN_TARGET       = $(GOBIN)/$(PROJECT_BIN)
DEB_TARGET       = $(BUILD_DIR)/$(PROJECT_BIN)_$(VERSION_STR)_$(ARCH).deb

.PHONY: test test-go test-ruby clean reset-deb-dir $(PROJECTS) $(PROJECT_DEBS)
.DEFAULT_GOAL := test

$(PROJECTS): $(GOBIN)
	$(eval proj := $@)
	go build -ldflags "$(LDFLAGS)" -o $(BIN_TARGET) $(PROJECT_PKG)

$(PROJECT_DEBS): LDFLAGS += -X github.com/Shopify/ghostferry.WebUiBasedir=/$(SHARE_DIR)
$(PROJECT_DEBS): reset-deb-dir
	$(eval proj := $(subst -deb,,$@))
	sed -e "s/{version}/$(VERSION_STR)/" -e "s/{arch}/$(ARCH)/" $(proj)/debian/control > $(DEB_PREFIX)/DEBIAN/control
	cp $(proj)/debian/copyright $(DEB_PREFIX)/DEBIAN/copyright
	go build -ldflags "$(LDFLAGS)" -o $(DEB_PREFIX)/$(BIN_DIR)/$(PROJECT_BIN) $(PROJECT_PKG)
	cp -ar webui $(DEB_PREFIX)/$(SHARE_DIR)
	if [ -d $(proj)/debian/files ]; then cp -ar $(proj)/debian/files/* $(DEB_PREFIX)/; fi
	dpkg-deb -b $(DEB_PREFIX) $(DEB_TARGET)

$(GOBIN):
	mkdir -p $(GOBIN)

test-go:
	@go version
	@if [ ! -f ./bin/gotestsum ]; then \
		mkdir -p ./bin; \
		curl -sL $(GOTESTSUM_URL) | tar -xz -C ./bin gotestsum; \
	fi

	ulimit -n 1024 && ./bin/gotestsum --format short-verbose ./test/go ./copydb/test ./sharding/test -count 1 -p 1 -failfast
	go test -v

test-ruby:
	bundle install
	bundle exec rake test

test: test-go test-ruby

clean:
	rm -rf build
	$(eval proj := *)
	rm -f $(BIN_TARGET)
	rm -f ./bin/gotestsum

reset-deb-dir:
	rm -rf $(DEB_PREFIX)
	mkdir -p $(DEB_PREFIX)/$(SHARE_DIR)
	mkdir -p $(DEB_PREFIX)/$(BIN_DIR)
	mkdir -p $(DEB_PREFIX)/DEBIAN

# Variables to be built into the binary
VERSION         := 1.1.0
DATETIME        := $(shell date -u +%Y%m%d%H%M%S)

ifndef IGNORE_DIRTY_TREE
DIRTY_TREE      := $(shell git diff-index --quiet HEAD -- || echo '+dirty')
endif

COMMIT          := $(addsuffix $(DIRTY_TREE),$(shell git rev-parse --short HEAD))
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

# Target specific variable, set proj to have a valid value.
PROJECT_PKG      = ./$(proj)/cmd
PROJECT_BIN      = ghostferry-$(proj)
BIN_TARGET       = $(GOBIN)/$(PROJECT_BIN)
DEB_TARGET       = $(BUILD_DIR)/$(PROJECT_BIN)_$(VERSION_STR).deb

PLATFORM        := $(shell uname -s | tr A-Z a-z)
GOTESTSUM_URL   := "https://github.com/gotestyourself/gotestsum/releases/download/v0.5.1/gotestsum_0.5.1_$(PLATFORM)_amd64.tar.gz"

.PHONY: test test-go test-ruby clean reset-deb-dir $(PROJECTS) $(PROJECT_DEBS)
.DEFAULT_GOAL := test

$(PROJECTS): $(GOBIN)
	$(eval proj := $@)
	go build -i -ldflags "$(LDFLAGS)" -o $(BIN_TARGET) $(PROJECT_PKG)

$(PROJECT_DEBS): LDFLAGS += -X github.com/Shopify/ghostferry.WebUiBasedir=/$(SHARE_DIR)
$(PROJECT_DEBS): reset-deb-dir
	$(eval proj := $(subst -deb,,$@))
	sed -e "s/{version}/$(VERSION_STR)/" $(proj)/debian/control > $(DEB_PREFIX)/DEBIAN/control
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
		mkdir ./bin; \
		curl -sL $(GOTESTSUM_URL) | tar -xz -C ./bin gotestsum; \
	fi

	ulimit -n 1024 && ./bin/gotestsum --format short-verbose ./test/go ./copydb/test ./sharding/test -count 1 -p 1

test-ruby:
	bundle install
	bundle exec rake test

test: test-go test-ruby

clean:
	rm -rf build
	$(eval proj := *)
	rm -f $(BIN_TARGET)

reset-deb-dir:
	rm -rf $(DEB_PREFIX)
	mkdir -p $(DEB_PREFIX)/$(SHARE_DIR)
	mkdir -p $(DEB_PREFIX)/$(BIN_DIR)
	mkdir -p $(DEB_PREFIX)/DEBIAN

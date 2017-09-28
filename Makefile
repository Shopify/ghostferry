.PHONY: test install clean copydb-deb reloc
.DEFAULT_GOAL := test

# Variables to be built into the binary
VERSION         := 1.0.0
DIRTY_TREE      := $(shell git diff-index --quiet HEAD -- || echo '+dirty')
COMMIT          := $(addsuffix $(DIRTY_TREE),$(shell git rev-parse --short HEAD))

# Flags
LDFLAGS         += -X github.com/Shopify/ghostferry.VersionNumber=$(VERSION)
LDFLAGS         += -X github.com/Shopify/ghostferry.VersionCommit=$(COMMIT)

# Paths
GOBIN           := $(GOPATH)/bin

COPYDB_TARGET   := $(GOBIN)/ghostferry-copydb
COPYDB_PKG      := ./copydb/cmd
COPYDB_DEB      := build/ghostferry-copydb.deb

RELOC_TARGET   := $(GOBIN)/reloc
RELOC_PKG      := ./reloc

SOURCES         := $(shell find . -name "*.go")

# Debian package paths
DEB_PREFIX  := build/debian
SHARE_DIR   := usr/share/ghostferry
BIN_DIR     := usr/bin

reloc: $(RELOC_TARGET)
$(RELOC_TARGET): $(GOBIN) $(SOURCES)
	go build -i -ldflags "$(LDFLAGS)" -o $(RELOC_TARGET) $(RELOC_PKG)

$(COPYDB_TARGET): $(GOBIN) $(SOURCES)
	go build -i -ldflags "$(LDFLAGS)" -o $(COPYDB_TARGET) $(COPYDB_PKG)

$(GOBIN):
	mkdir -p $(GOBIN)

copydb-deb: LDFLAGS += -X github.com/Shopify/ghostferry.WebUiBasedir=/$(SHARE_DIR)
copydb-deb: $(BUILD_DIR)
	rm -rf $(DEB_PREFIX)
	mkdir -p $(DEB_PREFIX)/$(SHARE_DIR)
	mkdir -p $(DEB_PREFIX)/$(BIN_DIR)
	mkdir -p $(DEB_PREFIX)/DEBIAN
	sed -e "s/{version}/$(VERSION)+$(COMMIT)/" debian/control > $(DEB_PREFIX)/DEBIAN/control
	go build -ldflags "$(LDFLAGS)" -o $(DEB_PREFIX)/$(BIN_DIR)/ghostferry-copydb $(COPYDB_PKG)
	cp -ar webui $(DEB_PREFIX)/$(SHARE_DIR)
	dpkg-deb -b $(DEB_PREFIX) $(COPYDB_DEB)

test:
	@go version
	go test ./test $(TESTFLAGS)

clean:
	rm -rf build
	rm -f $(COPYDB_TARGET)
	rm -f $(RELOC_TARGET)

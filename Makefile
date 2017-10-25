# Variables to be built into the binary
VERSION         := 1.1.0
DATETIME        := $(shell date -u +%Y%m%d%H%M%S)
DIRTY_TREE      := $(shell git diff-index --quiet HEAD -- || echo '+dirty')
COMMIT          := $(addsuffix $(DIRTY_TREE),$(shell git rev-parse --short HEAD))
DEB_VERSION     := $(VERSION)+$(DATETIME)+$(COMMIT)

# Flags
LDFLAGS         += -X github.com/Shopify/ghostferry.VersionNumber=$(VERSION)
LDFLAGS         += -X github.com/Shopify/ghostferry.VersionCommit=$(COMMIT)

# Paths
GOBIN           := $(GOPATH)/bin
BUILD_DIR       := build
DEB_PREFIX      := $(BUILD_DIR)/debian
SHARE_DIR       := usr/share/ghostferry
BIN_DIR         := usr/bin

# Targets
PROJECTS        := copydb reloc
PROJECT_DEBS    := $(foreach name,$(PROJECTS),$(name)-deb)

# Target specific variable, set proj to have a valid value.
PROJECT_PKG      = ./$(proj)/cmd
PROJECT_BIN      = ghostferry-$(proj)
BIN_TARGET       = $(GOBIN)/$(PROJECT_BIN)
DEB_TARGET       = $(BUILD_DIR)/$(PROJECT_BIN)_$(DEB_VERSION).deb

.PHONY: test clean reset-deb-dir $(PROJECTS) $(PROJECT_DEBS)
.DEFAULT_GOAL := test

$(PROJECTS): $(GOBIN) $(SOURCES) 
	$(eval proj := $@)
	go build -i -ldflags "$(LDFLAGS)" -o $(BIN_TARGET) $(PROJECT_PKG)

$(PROJECT_DEBS): LDFLAGS += -X github.com/Shopify/ghostferry.WebUiBasedir=/$(SHARE_DIR)
$(PROJECT_DEBS): reset-deb-dir
	$(eval proj := $(subst -deb,,$@))
	sed -e "s/{version}/$(DEB_VERSION)/" $(proj)/debian/control > $(DEB_PREFIX)/DEBIAN/control
	go build -ldflags "$(LDFLAGS)" -o $(DEB_PREFIX)/$(BIN_DIR)/$(PROJECT_BIN) $(PROJECT_PKG)
	cp -ar webui $(DEB_PREFIX)/$(SHARE_DIR)
	dpkg-deb -b $(DEB_PREFIX) $(DEB_TARGET)

$(GOBIN):
	mkdir -p $(GOBIN)

test:
	@go version
	go test `glide nv` -p 1 $(TESTFLAGS)

clean:
	rm -rf build
	$(eval proj := *)
	rm -f $(BIN_TARGET)

reset-deb-dir:
	rm -rf $(DEB_PREFIX)
	mkdir -p $(DEB_PREFIX)/$(SHARE_DIR)
	mkdir -p $(DEB_PREFIX)/$(BIN_DIR)
	mkdir -p $(DEB_PREFIX)/DEBIAN

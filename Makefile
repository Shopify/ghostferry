.PHONY: test install clean

GOBIN           := $(GOPATH)/bin

COPYDB_TARGET   := $(GOBIN)/ghostferry-copydb
COPYDB_PKG      := ./copydb/cmd

SOURCES       := $(shell find . -name "*.go")

define go_build_i
	go build -i -o $(1) $(2)
endef

install: $(COPYDB_TARGET)

$(COPYDB_TARGET): $(GOBIN) $(SOURCES)
	$(call go_build_i,$(COPYDB_TARGET),$(COPYDB_PKG))

$(GOBIN):
	mkdir -p $(GOBIN)

test:
	@go version
	go test ./test $(TESTFLAGS)

clean:
	rm -f $(COPYDB_TARGET)

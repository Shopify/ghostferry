.PHONY: test

BUILD_DIR     := build
COPYDB_BIN    := ghostferry-copydb
COPYDB_TARGET := $(BUILD_DIR)/$(COPYDB_BIN)

$(COPYDB_TARGET): $(BUILD_DIR)
	@go version
	go build -o $(BUILD_DIR)/$(COPYDB_BIN) ./copydb/cmd


$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)


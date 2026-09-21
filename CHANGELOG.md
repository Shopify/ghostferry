# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Introduce `BinlogCoordinate` for MySQL file/position and GTID-set coordinates, with zero-value detection and typed JSON serialization. Decoding also accepts the legacy bare `mysql.Position` JSON format.
- Add coordinate-based APIs to `BinlogStreamer`, `StateTracker`, and `SerializableState`, retaining the existing file/position APIs and resume behavior.
- Experimental MySQL GTID binlog streaming with `BinlogCoordinateMode: "gtid"` for source and target-verification streams. File/position streaming remains the default.
- Add helpers to read `@@GLOBAL.GTID_EXECUTED` as a GTID coordinate and check that the server's `gtid_mode` is `ON`.
- GTID cutover uses set containment. GTID mode requires `gtid_mode=ON` on the source and, when target verification is enabled, on the target. Resuming from serialized state is not yet supported in GTID mode and is explicitly rejected.

### Changed

- Extend `DMLEvent` with `BinlogCoordinate()` and `ResumableBinlogCoordinate()`. `DMLEventBase` implements both; custom implementations must provide these methods.
- Replace `BinlogCoordinate.Compare` with `HasReached`, using file/position ordering or GTID-set containment. Comparing different coordinate types now returns an error instead of panicking.
- Compare parsed GTID sets directly during cutover, eliminating per-event GTID serialization and parsing.
- Updated golang to 1.27.2
- Updated go packages:
  - `github.com/sirupsen/logrus` to v1.10.2
  - `github.com/rs/zerolog` to v1.35.1
  - `github.com/gorilla/mux` to v1.8.1
  - `github.com/golang/snappy` to v1.0.0
  - `github.com/DataDog/datadog-go` to v4.8.3
  - `github.com/stretchr/testify` to v1.12.1 (test dependency)
  - indirect: `golang.org/x/sys`, `golang.org/x/text`, `filippo.io/edwards25519`, `go.uber.org/zap`, `go.uber.org/atomic`
- Updated ruby to 4.0.7 and bundler to 4.0.22 (test dependency)
- Updated ruby gems, including `minitest` to v6 and `minitest-fail-fast` to v0.2 (test and docs dependencies)

### Fixed

- Reject malformed typed binlog coordinates instead of silently decoding them as an unset position.
- Clear cached GTID sets and inactive fields when decoding into an existing `BinlogCoordinate`, preventing stale reachability results.
- Keep GTID cutover aligned with transaction boundaries, including savepoints, DDL, empty transaction commits, and XA prepare/one-phase commit markers, without bypassing coordinate tracking when custom event handlers are registered.
- Synchronize GTID coordinate snapshots and updates across goroutines.
- Finish replayed GTID transactions before stopping at cutover, even when their GTIDs were already streamed before a reconnect.
- Publish binlog stop requests atomically after recording the stop coordinate, including reads from the control server.

## [1.3.1 - 2026-04-15]

### Changed

- Align default zerolog log level with logrus (info vs trace)
- Introduce slog handler wrapper around our Logger interface and use it with BinlogStreamer
- Updated golang to 1.26.2
- Updated ruby to 3.4.8 (test dependency)
- Updated github actions (CI dependency)

### Removed

- Vendored packages are removed in favour of normal `go mod`

## [1.3.0 - 2026-04-08]

### Changed

- Replace direct `logrus` usage with `Logger` interface, which uses `logrus` as a default backend.

### Added

- New configuration options for `LogBackend` and `LogLevel`, set with either environment variables or configuration passed in via stdin
- New logger backend: `zerolog`, which should be backwards compatible with `logrus`.

## [1.2.1 - 2026-02-12]

### Added

- `PaginationKey` now includes column name

### Changed

- Use `PaginationKey` instead of raw `uint64` for progress report. This means that table progress report will
  include not raw value, but a whole `PaginationKey` object, i.e.
  ```json
  {
    "type": "uint64",
    "column": "id",
    "value": 999
  }
  ```
  which will be in line with the format of state dump. @driv3r #426

## [1.2.0 - 2026-02-06]

### Added

- Changelog.
- UUID as ID: validate collation by @grodowski in #422
- NewPaginationKeyFromRow refactor by @grodowski in #424
- Pagination beyond uint64 by @milanatshopify in #417

## [1.1.0]

Past releases.

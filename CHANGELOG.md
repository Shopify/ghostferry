# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Introduce `BinlogCoordinate` for MySQL file/position and GTID-set coordinates, with zero-value detection and typed JSON serialization. Decoding also accepts the legacy bare `mysql.Position` JSON format.
- Add coordinate-based APIs to `BinlogStreamer`, `StateTracker`, and `SerializableState`, retaining the existing file/position APIs and resume behavior.
- Add `BinlogCoordinateMode` with `file_position` as the default and experimental MySQL-only `gtid` as a valid option. Binlog streaming remains file/position-based; GTID streaming is not yet supported.
- Add helpers to read `@@GLOBAL.GTID_EXECUTED` as a GTID coordinate and check that the server's `gtid_mode` is `ON`.

### Changed

- Extend `DMLEvent` with `BinlogCoordinate()` and `ResumableBinlogCoordinate()`. `DMLEventBase` implements both; custom implementations must provide these methods.
- Replace `BinlogCoordinate.Compare` with `HasReached`, using file/position ordering or GTID-set containment. Comparing different coordinate types now returns an error instead of panicking.

### Fixed

- Reject malformed typed binlog coordinates instead of silently decoding them as an unset position.
- Clear cached GTID sets and inactive fields when decoding into an existing `BinlogCoordinate`, preventing stale reachability results.

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

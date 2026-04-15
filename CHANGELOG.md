# Changelog

All notable changes to this project will be documented in this file.

## [1.3.1 - 2026-04-15]

### Changed

- Align default zerolog log level with logrus (info vs trace)
- Introduce slog handler wrapper around our Logger interface and use it with BinlogStreamer

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

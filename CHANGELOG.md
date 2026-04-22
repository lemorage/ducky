# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] - 2026-04-21

### Added
- Prepared statement API: `prepare`, `execute`, `finalize`, `with_statement`
- Bulk append via DuckDB's appender API: `append_rows`
- `exec` function for DDL/DML statements that return no rows
- Parameter constructor functions matching Gleam DB conventions: `int()`, `float()`, `text()`, `blob()`, `bool()`, `null()`, `nullable()`, `timestamp()`, `date()`, `time()`, `interval()`, `decimal()`
- UNION type support with tagged value decoding

### Changed
- **BREAKING:** Removed unused `Timeout` and `TypeMismatch` error variants
- **BREAKING:** Decode failures now return `Error` instead of silently converting to `Null`
- `UnsupportedParameterType` error now reports the specific type name
- Docs and examples updated to use `exec` and parameter constructors

### Fixed
- Blob encoding now returns `Blob(BitArray)` instead of `List(Integer)`
- Blob parameter binding support added to NIF
- NIF name mismatch for health_check function

## [0.3.0] - 2026-02-02

### Added
- DECIMAL type with lossless string encoding
- ENUM type support
- ARRAY and MAP types for query results
- Timestamp, Date, Time parameter binding
- Interval type with parameter binding
- HTTP proxy support for NIF downloads

### Changed
- **BREAKING:** Public API consolidated into single `ducky` module
- Error decoder now uses proper dynamic decoders

### Fixed
- Silent NULL conversion for unsupported parameter types

## [0.2.1] - 2026-01-25

### Fixed
- **CRITICAL:** Removed `rebar.config` from package that caused build failures
  `error: there is no code in this directory`.

The file triggered rebar3 compilation but package structure was incompatible.
v0.2.0 was unusable. Upgrade to v0.2.1 immediately.

### Changed
- NIF binary now auto-downloads at runtime on first use

## [0.2.0] - 2026-01-24

### Added
- Pre-built binaries for 5 platforms (macOS ARM64/x64, Linux ARM64/x64, Windows x64)
- Automatic NIF fetch at build time with source compilation fallback
- STRUCT types with `field()` accessor for composite data
- Temporal types: DATE, TIME, TIMESTAMP, INTERVAL
- LIST types with recursive nesting
- Example: `complex_types` demonstrating advanced type usage

## [0.1.0] - 2026-01-16

### Added
- Query execution: `query()` and `query_params()`
- Connection management: `connect()` and `close()`
- Automatic cleanup: `with_connection()` (recommended)
- Transactions: `transaction()` with auto-commit/rollback
- Type mappings: Null, Boolean, Integer, BigInt, Float, Double, Text, Blob
- DataFrame results with typed rows
- Error handling with descriptive variants

### Security
- Parameterized queries prevent SQL injection

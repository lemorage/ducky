# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

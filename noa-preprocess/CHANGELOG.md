# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Store clipped in different folder. Remove clipped prefix (#47)

## [0.2.0] - 2024-09-26
### Added
- Generic zip extraction to facilitate Sentinel 3 archives (#55)
- Custom extraction options (tiles, simple). Recursive clipping (#58)
- Extract and convert tif to COG config option (#40)

### Changed
- Config for clipping no longer faux-optional (#58)
- Extracting on Tile/Year/Month/Day folder tree (#43)

## [0.1.0] - 2024-06-26
### Added
- Initial commit - Extracting and clipping (#29)

### Changed
### Deprecated
### Removed
### Fixed
### Security
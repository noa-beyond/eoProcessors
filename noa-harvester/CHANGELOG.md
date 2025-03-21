# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
## [0.11.2] - 2024-12-18
### Fixed
- Fix some logs. Send one time the completed kafka message (#100)

## [0.11.1] - 2024-12-16
### Fixed
- Misleading error message and some logs (#98)

## [0.11.0] - 2024-12-16
### Changed
- Added env var fallback for Copernicus credentials (#96)

## [0.10.5] - 2024-12-13
### Fixed
- Kafka json bug (#94)

## [0.10.4] - 2024-12-11
### Fixed
- External bug: read proper key (#90)

## [0.10.3] - 2024-12-04
### Fixed
- Bug downloading one item at a time (#86)

## [0.10.2] - 2024-12-04
### Fixed
- UUID to str bug (#84)

## [0.10.1] - 2024-12-04
### Fixed
- Pass kafka topics as a list, not as a nested list (#82)

## [0.10.0] - 2024-12-02
### Fixed
- Some minor hot-fixes (#77)

## [0.9.0] - 2024-11-21
### Added
- Added publishing to kafka topic the successful/failed id lists (#75)
- Added consuming from kafka topic, getting id lists for download (#75)
- Added new cli command (noa_harvester_service) (#75)

### Changed
- Downloading from id (products table, column id) now returns successful and failed lists (#75)

## [0.8.0] - 2024-11-08
### Added
- Support for uuid list download of CDSE. Gateway CLI support and postgres read/update (#67)
### Changed
- Docker compose now also has secrets for db connection
- Updated python version: 3.12.0

## [0.7.0] - 2024-10-25
### Changed
- Bump version of CDSEtool to include bug fix of not just appending .zip (https://github.com/CDSETool/CDSETool/issues/180)

## [0.6.0] - 2024-09-24
### Added
- Introduced output folder cli option for downloading on Harvester (#32)
- Accept shapefile with multipolygon. Query for every polygon separately (#34)
### Changed
- Introduce option for drawing total bbox in multipolygon shapefiles (#36)

## [0.5.1-beta] - 2024-06-26
### Added
- Introduced assets selection for earthsearch (#27)

## [0.5.0] - 2024-06-26
### Added
- Introduce STAC search/download for Element84 COGs collections (#23)

## [0.4.0] - 2024-05-14
### Added
- Functionality before the creation of this CHANGELOG (#4, #14, #19)
- Integration of earthaccess (NASA) and cdsetool libs for Copernicus (ESA) asset downloads (#6, #11)
- Introduced bbox definition from a shapefile instead of the config key (#21)

### Changed
### Deprecated
### Removed
### Fixed
### Security
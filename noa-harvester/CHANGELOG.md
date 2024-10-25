# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
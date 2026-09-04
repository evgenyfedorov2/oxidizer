# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.24.1] - 2026-09-04

### Fixed

- `#[event(...)]` now rejects a field holding a mutable reference (`&mut T` or
  `Option<&mut T>`) while parsing, naming the offending field, rather than
  accepting it and failing later inside the generated code. Event fields are read
  through `&self` when the event is visited, so only shared references work.

- 🐛 Bug Fixes

  - reject mutable-reference event fields ([#730](https://github.com/microsoft/oxidizer/pull/730))

- ⚡ Performance

  - parallelize scheduled Miri and reduce resource outliers ([#706](https://github.com/microsoft/oxidizer/pull/706))

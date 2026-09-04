# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.25.0] - 2026-09-04

- ⚠️ Breaking

  - reject mutable-reference event fields instead of generating code that cannot access them through `&self` ([#730](https://github.com/microsoft/oxidizer/pull/730))

- 🔧 Maintenance

  - Now requires `0.24.1` of `observed_macros_impl`

- ♻️ Code Refactoring

  - split the implementation into observed_macros_impl ([#686](https://github.com/microsoft/oxidizer/pull/686))

# Agent instructions

## Release notes (required for every change)

- Every PR that changes behavior (features, fixes, dependency updates, config
  changes) MUST add an entry to `CHANGELOG.md` under the `## [Unreleased]`
  section. Use the Keep a Changelog subsections (`### Added`, `### Changed`,
  `### Fixed`, `### Removed`) and reference the PR number.
- Before a release, notes accumulate in the `[Unreleased]` section. At release
  time, the `[Unreleased]` section is switched to the new release version: rename
  it to `## [X.Y.Z] - YYYY-MM-DD` and add a fresh empty `## [Unreleased]` section
  above it.

## Release process

1. Pick the new version `X.Y.Z` following semantic versioning.
2. Bump `version` in the `[workspace.package]` section of the root `Cargo.toml`
   and run `cargo update --workspace` so `Cargo.lock` picks up the new version.
3. In `CHANGELOG.md`, rename `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD` and
   add a fresh empty `## [Unreleased]` section above it.
4. Open a PR with these changes; merge it.
5. Tag the merge commit on `main` as `vX.Y.Z` and push the tag:
   `git tag vX.Y.Z && git push origin vX.Y.Z`.
6. The `release` GitHub Actions workflow creates the GitHub release
   automatically, using the `X.Y.Z` section of `CHANGELOG.md` as the release
   notes.

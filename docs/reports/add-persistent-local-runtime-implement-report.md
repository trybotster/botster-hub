# Persistent local runtime implementation report

This historical implementation established daemon reuse, durable package state,
and first-party app launch over one data directory. The current production
contract supersedes its original bootstrap launcher:

- Operators install and enable packages through `packages install --path` and
  `packages enable`.
- Bare `up` refreshes persisted direct local package sources.
- `up` requires the exact enabled `botster-web` and `botster-tui` identities.
- Hub starts `botster-web/web-client` through generic entrypoint supervision,
  consumes child-authored structured `local_url`, and derives health/UI probes
  from that URL.
- `apps open` resolves Web and TUI launch contracts from daemon app state.

There is no compatibility launcher, checkout discovery, sibling-path default,
Hub-selected Web port, or second bridge owner. Git history retains the original
implementation details.

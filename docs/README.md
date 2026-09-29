# Documentation

Living documents:

- `client-protocol.md`: the Hub client protocol.
- `plans/plugin-platform.md`: the plugin platform design.
- `plans/readiness-and-flow-control.md`: the readiness and flow-control design.
- `lua-plugin-abi.md`: the Lua plugin API.
- `adr/`: decision records.
- `event-plane-load-proof.md`, `hub-resource-proof.md`, `lifecycle-suite-harness.md`,
  `loaded-daemon-lifecycle-runner.md`: references for the proof scripts.

Finished plans and reports were deleted. Git keeps every one of them. The last commit that contains
them is `2a61d032`. To read or restore one file:

    git show 2a61d032:docs/plans/<name>.md
    git checkout 2a61d032 -- docs/reports/<name>

Two files stay in `plans/` and `reports/` because a test reads them:
`plans/prove-the-event-plane-cannot-lag-or-block-hub-operations.md` and its calibration report.
They go when the event-plane saturation suite goes. That plan cites deleted files as `path@2a61d032`.

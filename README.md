# botster-hub

`botster-hub` is the Botster product host around `botster-core`.

The hub owns product policy and host integration:

- auth and identity policy
- config locations and persistence policy
- plugin/provider install and update policy
- cloud federation, signaling, and API adapters
- process supervision policy around embedded core sessions
- client transport adapters for browser, TUI, and other clients

The hub should embed `botster-core` for the reusable tmux-like local engine:
session spawning, PTY/process mechanics, session lifecycle and activity,
subscription fanout, notifications, plugin worker primitives, and consumer
conformance behavior.

This repo is intentionally greenfield. The existing `trybotster` monolith is
evidence only, not source to copy.

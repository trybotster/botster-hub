# Implement report: Hub project session state without blocking operation paths

## Target

- Repository: `botster-hub`
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1786663582_169720`
- Run: `run_1786689005_381068`
- Implement step: `run_step_1786766740_942975`
- Review return: `review_1786766726_101839`
- Approved plan: `docs/plans/project-session-state-without-blocking-operation-paths.md` (`c4fad52`)
- `teardown_class_applies`: no
- Delivery: direct-merge, no pull request
- Implement commit: `424902bf5443b0725a373161cd956508ca0001a2`

## Playbooks and notes applied

- `implementer-playbook`
- `botster-implementer-playbook`
- `botster-hub-playbook`
- `Hub owner loop calls bounded Core lifecycle page APIs`
- `Hub session projection continues without subscribers or terminal Drain`
- `Hub owner loop wakes only for mutations and pending resync`
- `Hub synchronizes plugin workers with session lifecycle events and a baseline`
- `botster hub events use bounded priority lanes instead of unbounded queue fuses`
- `incremental attach snapshot frames require lossless streaming backpressure`
- `test script required for rust tests not cargo test`
- `implement gate must verify committed work and pr link before review`
- `implementation artifacts must match actual git state`
- `implementation steps must persist report artifacts for review`
- `rust repo strict lints must be verified before dismissing warnings`
- `pipeline vault checklists must cite exact resolvable note titles`

Ambient SessionStart mapped rails/general. This run used the ticket target `botster-hub`.

## Files changed

- `src/daemon_entity_subscriptions.rs`
- `src/daemon_maintenance.rs`
- `src/lifecycle.rs`
- `src/runtime.rs`
- `src/config.rs`
- `src/lib.rs`
- `docs/client-protocol.md`
- `tests/external_core_engine_options_construct.rs`
- `tests/hub_plugin_lifecycle_test.rs`
- `tests/hub_daemon_lifecycle/plugin_bounds.rs`
- `docs/reports/project-session-state-without-blocking-operation-paths-implement.md`

## Ownership boundaries preserved

Hub owns owner-loop slices, the session projection, session subscriber delivery, and `/session` host-bridge admission. Core still owns page APIs. Terminal bytes, Workspaces/membership, and client DTO variants are unchanged.

## Cross-repo routing

No Web or TUI checkout. The first session snapshot is one bounded page. Remaining rows arrive as upserts. An oversize page sends one error and closes the subscription.

## Deviations from plan

1. `origin/main` pins Core `f4f6bf5` (13 Aug). That revision does not expose the sliced lifecycle APIs. This ticket keeps one Git-visible pin at `aef6516` (14 Aug).
2. Class-specific worker-queue knobs live on nested `PluginWorkerClassOptions`. Supported construction is `CoreEngineOptions::new(...)` or `..Default`.

## Tests and downstream proof

Ran from this checkout with one local target and no `CARGO_TARGET_DIR`:

1. `cargo fmt --all`
2. `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
3. `cargo build --locked --offline -p botster-core-daemon --bin botster-session-worker`
4. `./test.sh --locked --offline`

## Provenance

- Core lock: `aef6516d5809d563961ed7fdd07da29a7b4edddc`
- Merged main: `959c58f55726d098299cced8af151d8f496f41e3`
- Parent implement: `be254d407fe40730cafbdf33bab2baa88eec9d2f`
- `target/debug/botster-hub`
- `target/debug/botster-session-worker`

## Unverified behavior or residual risk

- A 256-process live daemon was not started. Large-registry proofs remain unit tests.
- Web and TUI were not rebuilt. They already apply snapshot then upsert/patch/remove.
- The WebRTC write-budget sibling-output check can fail under parallel suite load.

## Missing vault guidance

None new.

## Review findings addressed

- `finding_1786766726_834551`: First snapshot is one bounded page. Remaining rows stream as upserts. Removal discovery visits a bounded cursor of rows, not only matched removals.
- `finding_1786766726_264222`: HostBridge uses one shared visit budget and one shared 25 ms deadline. Handler paging counts visited map keys. Consumer removal uses a persistent prune cursor. `run_host_bridge_slice` is tested with matching and nonmatching plugins.
- `finding_1786766726_104000`: The global fanout queue has item and byte caps. Pressure clears the queue and starts paged baseline recovery.
- `finding_1786766726_429708`: An oversize snapshot page sends one error and closes the subscription.
- `finding_1786766726_552511`: New knobs are nested on `PluginWorkerClassOptions`.
- `finding_1786766726_704155`: This report records the exact Git commit after the commit lands.

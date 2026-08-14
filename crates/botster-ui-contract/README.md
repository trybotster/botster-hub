# botster-ui-contract

Renderer-neutral Botster plugin UI contract.

The Rust consumer identity is the Hub Git tag `botster-ui-contract-v0.3.2`.
That tag is the same UI contract version as npm `@trybotster/ui-contract@0.3.2`.
Do not pin this crate from a Hub commit SHA, a `rev`, or crates.io.

```toml
[dependencies]
botster-ui-contract = { git = "https://github.com/trybotster/botster-hub.git", tag = "botster-ui-contract-v0.3.2" }
```

Git `botster-hub-client` and `botster-hub-test-support` depend on this tag.
The Hub workspace path-resolves the crate for local development only through a
workspace `[patch]` entry.

Create or verify the tag with `script/tag-ui-contract` from a clean Hub
checkout on the merged main commit. The script does not publish crates.io and
does not change the npm package.

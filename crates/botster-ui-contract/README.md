# botster-ui-contract

Renderer-neutral Botster plugin UI contract.

The consumer identity is crates.io `botster-ui-contract = "0.3.2"`. That
coordinate is the same UI contract version as npm `@trybotster/ui-contract@0.3.2`.
Do not pin this crate from a Hub Git SHA.

```toml
[dependencies]
botster-ui-contract = "0.3.2"
```

Git `botster-hub-client` and `botster-hub-test-support` depend on this crates.io
version. The Hub workspace path-resolves the crate for local development only
through a workspace `[patch.crates-io]` entry.

Publish npm and crates.io together with `script/publish-npm-packages` from a
clean Hub checkout. The script refuses version drift, skips a published
coordinate whose integrity matches, and refuses to overwrite a published
coordinate whose integrity differs.

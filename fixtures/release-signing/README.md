# UNTRUSTED TEST-ONLY release signing material

These keys exist so `./test.sh` can exercise the real signature path end to end.
They are **not** production material and must never be treated as such:

- The private key is committed to a public repository. Anyone can sign anything
  with it.
- The `key_id` recorded in generated fixtures is `test-only-do-not-trust`.
- The installer embeds **no** trust anchor and requires an explicit
  `--trust-anchor <path>`. It refuses to run without one, so nothing can pick
  this key up by accident.

Shipping a default embedded key in this ticket was deliberately avoided for
exactly that reason. Real key custody, real key rotation, and a real HTTPS
origin belong to the publication ticket, not this one.

Regenerate with:

```sh
cargo run -p botster-hub-installer --bin botster-hub-release-tool -- \
  generate-key --out-dir fixtures/release-signing \
  --name UNTRUSTED-TEST-ONLY-botster-hub-release-signing
```

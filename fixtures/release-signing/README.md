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

## Rotating this pair

`generate-key` refuses to overwrite existing key material and creates the
private key `0600`, so rotation is a deliberate two-step act rather than a
silent side effect of re-running a command:

```sh
rm fixtures/release-signing/UNTRUSTED-TEST-ONLY-botster-hub-release-signing.{pub,pkcs8}
cargo run -p botster-hub-installer --bin botster-hub-release-tool -- \
  generate-key --out-dir fixtures/release-signing \
  --name UNTRUSTED-TEST-ONLY-botster-hub-release-signing
```

Git records only the executable bit, so the committed private key lands at the
checkout's default mode. That is acceptable *only* because this key is
untrusted by construction; a real signing key must never be committed.

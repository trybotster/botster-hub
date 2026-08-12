# Frozen GHOSTSNP late-attach goldens

Generated under locked pins:

- Core: `2c5171a6cb3b073c53620a9838d8b08480dd215c`
- Ghostty submodule: `5e9ba17a22ba8e40bf8de7d3e7555b8378cb1880`
- Terminal size: 24×80

| File | Role | Recipe |
| --- | --- | --- |
| `late-attach-history-marker-v1.ghostsnp` | Golden A / history_then_live Snapshot | Create Ghostty terminal, write `history-before-live\r\n`, export GHOSTSNP once |
| `late-attach-blank-v1.ghostsnp` | Golden B / no_history_then_live Snapshot | Create Ghostty terminal, zero writes, export GHOSTSNP immediately |

Do not dual-use a history-bearing golden as no-history. SHAs must differ.
Bytes are frozen; do not regenerate at package build time.

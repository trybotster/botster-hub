#!/bin/sh
# Writes the candidate environment of test.sh --phase build as a file that
# `. <file>` restores exactly: every value is single-quoted, and a single quote
# inside a value is written as '\''.
shell_quote() {
  printf "'"
  printf '%s' "$1" | sed "s/'/'\\\\''/g"
  printf "'"
}

write_candidate_env() {
  {
    for name in BOTSTER_HUB_BIN BOTSTER_SESSION_WORKER_BIN BOTSTER_CANDIDATE_MANIFEST BOTSTER_HUB_CLIENT_ADAPTER_BIN; do
      eval "value=\${$name}"
      printf 'export %s=' "$name"
      shell_quote "$value"
      printf '\n'
    done
  } >"$1"
}

#!/usr/bin/env sh
set -eu

BOTSTER_ENV=test cargo test "$@"

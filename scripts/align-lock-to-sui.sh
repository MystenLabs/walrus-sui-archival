#!/usr/bin/env bash
# Pin crates that must match Sui's own Cargo.lock to the versions Sui locks at
# the tag currently pinned in Cargo.toml.
#
# Some crates shared with Sui only build at the exact version Sui locks (for
# example `allocative`, which `starlark_map` 0.13 cannot build against once it
# moves past 0.3.4). A plain `cargo update` happily bumps them; this script
# moves them back. Only versions in the same semver-compatible range are
# touched, since `cargo update --precise` cannot cross a breaking boundary.
#
# Usage: scripts/align-lock-to-sui.sh [crate ...]
# With no arguments the default list below is used.

set -euo pipefail

DEFAULT_CRATES=(allocative)
CRATES=("$@")
if [ ${#CRATES[@]} -eq 0 ]; then
  CRATES=("${DEFAULT_CRATES[@]}")
fi

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"

SUI_TAG=$(grep -m1 -E 'MystenLabs/sui", tag = "' Cargo.toml | grep -oE 'testnet-v[0-9]+\.[0-9]+\.[0-9]+')
if [ -z "$SUI_TAG" ]; then
  echo "could not find the sui tag in Cargo.toml" >&2
  exit 1
fi

SUI_LOCK=$(mktemp)
trap 'rm -f "$SUI_LOCK"' EXIT
curl -sfL "https://raw.githubusercontent.com/MystenLabs/sui/$SUI_TAG/Cargo.lock" -o "$SUI_LOCK"

# Print every locked version of a crate, one per line.
versions() {
  awk -v c="$1" '$0 == "name = \"" c "\"" { getline; gsub(/version = |"/, ""); print }' "$2"
}

# The semver-compatible prefix of a version: "0.3" for 0.3.4, "57" for 57.3.1.
compat() {
  case "$1" in
    0.*) echo "$1" | cut -d. -f1-2 ;;
    *)   echo "$1" | cut -d. -f1 ;;
  esac
}

changed=0
for crate in "${CRATES[@]}"; do
  while read -r have; do
    [ -n "$have" ] || continue
    want=""
    while read -r candidate; do
      if [ "$(compat "$candidate")" = "$(compat "$have")" ]; then
        want="$candidate"
        break
      fi
    done < <(versions "$crate" "$SUI_LOCK")

    if [ -z "$want" ]; then
      echo "$crate $have: no compatible version in sui $SUI_TAG lock, leaving as is"
    elif [ "$want" = "$have" ]; then
      echo "$crate $have: already matches sui $SUI_TAG"
    else
      echo "$crate $have -> $want (sui $SUI_TAG)"
      cargo update -p "$crate@$have" --precise "$want"
      changed=1
    fi
  done < <(versions "$crate" Cargo.lock)
done

if [ "$changed" -eq 0 ]; then
  echo "lockfile already aligned with sui $SUI_TAG"
fi

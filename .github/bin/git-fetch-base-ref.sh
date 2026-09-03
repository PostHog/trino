#!/bin/bash

set -euo pipefail

if [ -z "${GITHUB_BASE_REF:-}" ] || [ "$GITHUB_BASE_REF" = "master" ]; then
    echo "GITHUB_BASE_REF is not set or is master, not fetching it" >&2
    exit 0
fi

git fetch --no-tags --prune origin "$GITHUB_BASE_REF"

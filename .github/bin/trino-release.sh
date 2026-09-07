#!/usr/bin/env bash

set -euo pipefail

fail() { printf '%s\n' "$*" >&2; exit 1; }

[[ "${GITHUB_REPOSITORY:-}" == PostHog/trino ]] || fail 'Only PostHog/trino can publish releases'
[[ "${GITHUB_REF:-}" == refs/heads/master ]] || fail 'Only master can publish releases'
[[ "${GITHUB_EVENT_NAME:-}" == push || "${GITHUB_EVENT_NAME:-}" == workflow_dispatch ]] || fail 'Unsupported release event'
[[ "${GITHUB_SHA:-}" =~ ^[0-9a-f]{40}$ ]] || fail 'A full source revision is required'
[[ "$(git rev-parse HEAD)" == "$GITHUB_SHA" ]] || fail 'Source revision does not match checkout'
[[ "$(git rev-parse --is-shallow-repository)" == false ]] || fail 'Complete source history is required'

position="$(git rev-list --first-parent --count HEAD)"
[[ "$position" =~ ^[1-9][0-9]{0,11}$ ]] || fail 'Source position is outside the release tag range'
printf -v ordered_tag 'r%012d-%.6s' "$position" "$GITHUB_SHA"
repository=ghcr.io/posthog/trino
source_url=https://github.com/PostHog/trino

scratch="$(mktemp -d)"
trap 'rm -r "$scratch"' EXIT

inspect_digest() {
    timeout --kill-after=10s 30s docker buildx imagetools inspect \
        --format '{{json .Manifest.Digest}}' "$1" | jq -er 'select(test("^sha256:[0-9a-f]{64}$"))'
}

existing_release() {
    local digest
    if digest="$(inspect_digest "$repository:$ordered_tag" 2> "$scratch/inspect-error")"; then
        timeout --kill-after=10s 30s docker buildx imagetools inspect --raw "$repository@$digest" |
            jq -e --arg revision "$GITHUB_SHA" --arg source "$source_url" '
                .annotations["org.opencontainers.image.revision"] == $revision and
                .annotations["org.opencontainers.image.source"] == $source
            ' >/dev/null || fail 'Existing ordered release has invalid provenance'
        printf '%s\n' "$digest"
    elif grep -Eqi 'manifest unknown' "$scratch/inspect-error" ||
        grep -Fqx "ERROR: $repository:$ordered_tag: not found" "$scratch/inspect-error"; then
        return 0
    else
        fail 'Cannot determine whether the ordered release exists'
    fi
}

case "${1:-}" in
    prepare)
        digest="$(existing_release)"
        printf 'ordered-tag=%s\ndigest=%s\n' "$ordered_tag" "$digest" >> "${GITHUB_OUTPUT:?}"
        ;;
    publish)
        readable_tag="${READABLE_TAG:-$GITHUB_SHA}"
        [[ "$readable_tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || fail 'Invalid image alias'
        [[ ! "$readable_tag" =~ ^r[0-9]{12}-[0-9a-f]{6}$ ]] || fail 'Readable tags cannot replace ordered releases'
        [[ ! "$readable_tag" =~ ^[0-9a-f]{40}$ || "$readable_tag" == "$GITHUB_SHA" ]] || fail 'Readable tags cannot replace another source revision'
        digest="$(existing_release)"
        if [[ -z "$digest" ]]; then
            [[ "${BUILD_DIGEST:-}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'Build digest is required for a new release'
            [[ "${GITHUB_RUN_ID:-}" =~ ^[0-9]+$ && "${GITHUB_RUN_ATTEMPT:-}" =~ ^[0-9]+$ ]] || fail 'Run identity is required'
            annotated_tag="build-metadata-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
            timeout --kill-after=10s 60s docker buildx imagetools create \
                --annotation "index:org.opencontainers.image.source=$source_url" \
                --annotation "index:org.opencontainers.image.revision=$GITHUB_SHA" \
                --tag "$repository:$annotated_tag" "$repository@$BUILD_DIGEST"
            digest="$(inspect_digest "$repository:$annotated_tag")"
            timeout --kill-after=10s 60s docker buildx imagetools create --prefer-index=false \
                --tag "$repository:$ordered_tag" "$repository@$digest"
            [[ "$(existing_release)" == "$digest" ]] || fail 'Ordered release read-back does not match the build'
        fi

        # Keep legacy aliases on the same verified artifact as the ordered tag.
        for tag in "$GITHUB_SHA" "$readable_tag"; do
            timeout --kill-after=10s 60s docker buildx imagetools create --prefer-index=false \
                --tag "$repository:$tag" "$repository@$digest"
            [[ "$(inspect_digest "$repository:$tag")" == "$digest" ]] || fail 'Legacy alias read-back does not match release'
        done
        printf 'digest=%s\n' "$digest" >> "${GITHUB_OUTPUT:?}"
        ;;
    *) fail 'Expected prepare or publish' ;;
esac

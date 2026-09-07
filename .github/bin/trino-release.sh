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
repository=795637471508.dkr.ecr.us-east-1.amazonaws.com/posthog-trino
mirror=ghcr.io/posthog/trino
source_url=https://github.com/PostHog/trino
build_tag="build-$GITHUB_SHA"

scratch="$(mktemp -d)"
trap 'rm -r "$scratch"' EXIT

inspect_digest() {
    timeout --kill-after=10s 30s docker buildx imagetools inspect \
        --format '{{json .Manifest.Digest}}' "$1" | jq -er 'select(test("^sha256:[0-9a-f]{64}$"))'
}

existing_digest() {
    local reference=$1
    local digest
    if digest="$(inspect_digest "$reference" 2> "$scratch/inspect-error")"; then
        printf '%s\n' "$digest"
    elif grep -Eqi 'manifest unknown' "$scratch/inspect-error" ||
        grep -Fqx "ERROR: $reference: not found" "$scratch/inspect-error"; then
        return 0
    else
        fail "Cannot determine whether $reference exists"
    fi
}

verify_manifest() {
    timeout --kill-after=10s 30s docker buildx imagetools inspect --raw "$1" |
        jq -e --arg revision "$GITHUB_SHA" --arg source "$source_url" --arg media_type "$2" '
            .mediaType == $media_type and
            .annotations["org.opencontainers.image.revision"] == $revision and
            .annotations["org.opencontainers.image.source"] == $source
        ' >/dev/null || fail 'Image has invalid media type or source provenance'
}

existing_release() {
    local digest
    digest="$(existing_digest "$repository:$ordered_tag")" || return 1
    if [[ -n "$digest" ]]; then
        verify_manifest "$repository@$digest" application/vnd.oci.image.index.v1+json || return 1
        printf '%s\n' "$digest"
    fi
}

ensure_alias() {
    local target=$1 digest=$2 replace=${3:-false}
    local existing
    existing="$(existing_digest "$target")" || return 1
    [[ "$existing" != "$digest" ]] || return 0
    [[ -z "$existing" || "$replace" == true ]] || fail "Immutable alias already contains a different image: $target"
    timeout --kill-after=10s 600s docker buildx imagetools create --prefer-index=false \
        --tag "$target" "$repository@$digest"
    [[ "$(inspect_digest "$target")" == "$digest" ]] || fail 'Alias read-back does not match release'
}

case "${1:-}" in
    prepare)
        digest="$(existing_release)"
        build_digest=
        if [[ -z "$digest" ]]; then
            build_digest="$(existing_digest "$repository:$build_tag")"
            if [[ -n "$build_digest" ]]; then
                verify_manifest "$repository@$build_digest" application/vnd.oci.image.manifest.v1+json
            fi
        fi
        printf 'ordered-tag=%s\ndigest=%s\nbuild-digest=%s\n' "$ordered_tag" "$digest" "$build_digest" >> "${GITHUB_OUTPUT:?}"
        ;;
    publish)
        readable_tag="${READABLE_TAG:-$GITHUB_SHA}"
        [[ "$readable_tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ ]] || fail 'Invalid image alias'
        [[ ! "$readable_tag" =~ ^r[0-9]{12}-[0-9a-f]{6}$ ]] || fail 'Readable tags cannot replace ordered releases'
        [[ ! "$readable_tag" =~ ^[0-9a-f]{40}$ || "$readable_tag" == "$GITHUB_SHA" ]] || fail 'Readable tags cannot replace another source revision'
        digest="$(existing_release)"
        if [[ -z "$digest" ]]; then
            [[ "${BUILD_DIGEST:-}" =~ ^sha256:[0-9a-f]{64}$ ]] || fail 'Build digest is required for a new release'
            verify_manifest "$repository@$BUILD_DIGEST" application/vnd.oci.image.manifest.v1+json
            timeout --kill-after=10s 60s docker buildx imagetools create \
                --annotation "index:org.opencontainers.image.source=$source_url" \
                --annotation "index:org.opencontainers.image.revision=$GITHUB_SHA" \
                --tag "$repository:$ordered_tag" "$repository@$BUILD_DIGEST"
            digest="$(existing_release)"
            [[ -n "$digest" ]] || fail 'Ordered release was not published'
        fi

        ensure_alias "$repository:$GITHUB_SHA" "$digest"
        ensure_alias "$mirror:$ordered_tag" "$digest"
        ensure_alias "$mirror:$GITHUB_SHA" "$digest"
        if [[ "$readable_tag" != "$GITHUB_SHA" ]]; then
            ensure_alias "$mirror:$readable_tag" "$digest" true
        fi
        verify_manifest "$mirror@$digest" application/vnd.oci.image.index.v1+json
        [[ "$(inspect_digest "$repository:$ordered_tag")" == "$digest" ]] || fail 'ECR release changed during publication'
        [[ "$(inspect_digest "$mirror:$ordered_tag")" == "$digest" ]] || fail 'GHCR release does not match ECR'
        printf 'digest=%s\n' "$digest" >> "${GITHUB_OUTPUT:?}"
        ;;
    *) fail 'Expected prepare or publish' ;;
esac

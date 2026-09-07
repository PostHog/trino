# PostHog image releases

The publisher runs only for pushes to `master` and manual dispatches on `master`.
Tag pushes no longer publish images. Use a manual dispatch on `master` to add a
readable release alias. Pull requests run contract tests without package-write
permission.

The ordered tag is `r<12-digit first-parent commit count>-<6-character revision>`.
The checkout must contain complete history and match the workflow revision.
This orders releases by source history, independently of build completion order.

The canonical image repository is `posthog-trino` in `us-east-1`.
The workflow derives its registry address from the authenticated ECR login.
The publisher mirrors the same image to `ghcr.io/posthog/trino` for existing
consumers. It builds once, then copies the image and its layers between
registries without rebuilding.

The image is wrapped in an OCI index carrying manifest-level
`org.opencontainers.image.source` and `org.opencontainers.image.revision`
annotations. The ordered tag, full revision alias, and optional readable alias
resolve to this same index digest. ECR receives only the ordered and full
revision aliases. Readable aliases remain GHCR-only. The existing charts state
dispatch receives the digest only after both registries pass read-back checks
and continues to run only after a push to `master`. Merging this workflow can
therefore roll the existing dev Trino deployment independently of new cells.

The publisher uses a Buildx `docker-container` builder and the registry exporter
with `oci-mediatypes=true`. The default local `core/docker/build.sh` behavior
stays unchanged. The publisher rejects non-OCI image manifests before it adds
annotations: Buildx does not add index annotations to Docker manifest lists.
See the [Buildx index creation implementation](https://github.com/docker/buildx/blob/master/util/imagetools/create.go)
and [BuildKit OCI exporter option](https://github.com/moby/buildkit/blob/master/exporter/containerimage/exptypes/keys.go).

Retries and manual runs reuse a verified ECR release and skip the build. If the
workflow stopped before it created the ordered index, it reuses the immutable
`build-<full-revision>` staging image instead. That image carries source and
revision annotations on its OCI manifest. Staging tags are not eligible
releases. If GHCR failed after ECR succeeded, a retry repairs only missing
aliases. Existing immutable aliases must contain exactly the expected digest;
the publisher never overwrites them. Only a GHCR readable alias can move.
Registry read failures or incorrect provenance stop publication. Do not delete
an ordered tag to force a rebuild; publish a new source commit instead.

Before merging, apply the separate infrastructure change that creates the
fully immutable ECR repository and the master-only publisher role. Configure the
organization Actions secret `AWS_ECR_PUBLISH_IAM_ROLE` with the publisher role ARN
and grant this repository access:
`arn:aws:iam::<AWS_ACCOUNT_ID>:role/github-trino-publish-role`. This is an operator
setup step, not a change performed by this workflow. The workflow masks the AWS
account ID in logs and does not store its registry address in source. OIDC trust must
allow only `repo:PostHog/trino:ref:refs/heads/master`. The role must allow image
push/read operations but no image deletion or repository-policy changes.

The workflow serializes its publishers. This prevents races within this
workflow, but does not itself establish registry immutability or exclusive
writers. Before enabling ECR release discovery, verify the applied repository
immutability, protected source history, repository access, and effective writer
permissions. GHCR remains a compatibility mirror and is not the new cells'
trusted release source. This change does not modify repository rules, package
access, Actions secrets, or registry settings.

Run the local contract tests with `python3 .github/bin/test_trino_release.py`.
These tests mock the registry commands; they do not publish images. After the
first authorized real publication, verify its ordered tag, index annotations,
and revision-alias digest through the registry before enabling deployment
discovery.

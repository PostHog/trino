# Truffle image releases

The publisher runs only for pushes to `master` and manual dispatches on `master`.
Tag pushes no longer publish images. Use a manual dispatch on `master` to add a
readable release alias. Pull requests run contract tests without package-write
permission.

The ordered tag is `r<12-digit first-parent commit count>-<6-character revision>`.
The checkout must contain complete history and match the workflow revision.
This orders releases by source history, independently of build completion order.

The image coordinates keep their pre-rename names on purpose: the ECR repository
`posthog-trino`, the GHCR mirror `ghcr.io/posthog/trino`, and the charts release
`trino` (`state/trino.yaml`). Deployments pin the image by digest under those
names, so renaming them is a separate migration that must publish to both names
while consumers move. Only the source repository identity changed, to
`PostHog/truffle`.

The canonical image repository is `posthog-trino` in `us-east-1`.
The workflow derives its registry address from the authenticated ECR login.
The publisher mirrors the same image to `ghcr.io/posthog/trino` for existing
consumers. It builds once, then copies the image and its layers between
registries without rebuilding.

The image is wrapped in an OCI index carrying manifest-level
`org.opencontainers.image.source` and `org.opencontainers.image.revision`
annotations. The ordered tag, full revision alias, and optional readable alias
resolve to this same index digest. ECR receives only the ordered and full
revision aliases. Readable aliases remain GHCR-only. The charts state dispatch runs only after a push to `master`. It sends the
digest after both registries pass the read-back checks. Keep the release key
`trino` because deployments read the digest from that state entry. The dispatch
sends the current GitHub repository name as source metadata.

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

The publisher requires a fully immutable ECR repository. Set the organization
Actions secret `AWS_ECR_PUBLISH_IAM_ROLE` to the publisher role ARN. Grant
`PostHog/truffle` access to the secret. The workflow masks the AWS account ID in
logs. It does not store the registry address in source.

The publisher role must allow only the GitHub OIDC subject for `master` in
`PostHog/truffle`. A repository rename can change this subject. GitHub uses
[immutable subject claims](https://docs.github.com/en/actions/reference/security/oidc#immutable-subject-claims)
for repository renames after July 15, 2026. These claims include the owner and
repository IDs in addition to their names. Check the current subject prefix in
the repository's [OIDC settings](https://docs.github.com/en/rest/actions/oidc#get-the-customization-template-for-an-oidc-subject-claim-for-a-repository)
before changing the trust policy. Scope trust to that exact prefix with the
`:ref:refs/heads/master` suffix.

The role must allow image pushes and reads. It must not allow image deletion
or repository policy changes. If `Configure AWS credentials` fails after a
rename, check the role trust before changing any image coordinates. Apply the
trust change before trying to publish again. Verify that the next push to `master` publishes the image. Check
that the charts state entry records the new digest and source repository. A
manual dispatch publishes the image without updating the charts state.

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

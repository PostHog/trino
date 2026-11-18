# Truffle image releases

The publisher runs only for pushes to `master` and manual dispatches on `master`.
Tag pushes no longer publish images. Use a manual dispatch on `master` to add a
readable release alias. Pull requests run contract tests without package-write
permission.

The ordered tag is `r<12-digit first-parent commit count>-<6-character revision>`.
The checkout must contain complete history and match the workflow revision.
This orders releases by source history, independently of build completion order.

The canonical image repository is `posthog-truffle` in shared-services ECR
in `us-east-1`. The workflow gets its registry address from the ECR login.
The workflow does not publish to `posthog-trino`.
It builds one image. It copies that image to `ghcr.io/posthog/trino` for
existing GHCR consumers.

The image is wrapped in an OCI index carrying manifest-level
`org.opencontainers.image.source` and `org.opencontainers.image.revision`
annotations. The ordered tag, full revision alias, and optional readable alias
resolve to this same index digest. ECR receives only the ordered and full
revision aliases. Readable aliases remain GHCR-only. A push to `master`
records the verified digest in `state/truffle.yaml` in charts. A manual
dispatch publishes the image without updating charts. The publisher does not
update `state/trino.yaml`, which still points to the old image.

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

Before merging, provision the immutable ECR repository and its publisher role.
Set the repository Actions secret `AWS_ECR_TRUFFLE_PUBLISH_IAM_ROLE` to the
new role ARN. The workflow masks the AWS account ID in logs.

Scope the OIDC trust to `PostHog/truffle` on `master`. Use the current
[OIDC subject prefix](https://docs.github.com/en/rest/actions/oidc#get-the-customization-template-for-an-oidc-subject-claim-for-a-repository)
with the suffix `:ref:refs/heads/master`. A repository rename can change the
prefix. Give the role permission to push and read images. Do not give it
permission to delete images or change the repository policy.

The workflow serializes its publishers. This prevents races within this
workflow, but does not itself establish registry immutability or exclusive
writers. Before enabling ECR release discovery, verify the applied repository
immutability, protected source history, repository access, and effective writer
permissions. GHCR remains a compatibility mirror and is not the new cells'
trusted release source. This change does not modify repository rules, package
access, Actions secrets, or registry settings.

Run the local contract tests with `python3 .github/bin/test_trino_release.py`.
These tests mock the registry commands; they do not publish images. After the
first publication, verify the digest in shared-services ECR in each deployment
region. Check that `state/truffle.yaml` records that digest. Then switch the
deployments to the new image repository and state file.

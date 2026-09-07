# PostHog image releases

The publisher runs only for pushes to `master` and manual dispatches on `master`.
Tag pushes no longer publish images. Use a manual dispatch on `master` to add a
readable release alias. Pull requests run contract tests without package-write
permission.

The ordered tag is `r<12-digit first-parent commit count>-<6-character revision>`.
The checkout must contain complete history and match the workflow revision.
This orders releases by source history, independently of build completion order.

The image is wrapped in an OCI index carrying manifest-level
`org.opencontainers.image.source` and `org.opencontainers.image.revision`
annotations. The ordered tag, full revision alias, and optional readable alias
resolve to this same index digest. The existing charts state dispatch receives
that digest and continues to run only after a push to `master`.

The publisher uses a Buildx `docker-container` builder and the registry exporter
with `oci-mediatypes=true`. The default local `core/docker/build.sh` behavior
stays unchanged. The publisher rejects non-OCI image manifests before it adds
annotations: Buildx does not add index annotations to Docker manifest lists.
See the [Buildx index creation implementation](https://github.com/docker/buildx/blob/master/util/imagetools/create.go)
and [BuildKit OCI exporter option](https://github.com/moby/buildkit/blob/master/exporter/containerimage/exptypes/keys.go).

Retries and manual runs for an already published source revision reuse its
verified ordered release digest and skip the build. They never replace that
ordered tag. Registry read failures or incorrect provenance stop publication.
Unique staging tags do not match the release selector and are not eligible
releases. Do not delete an ordered tag to force a rebuild; publish a new source
commit instead.

The workflow serializes its publishers. This prevents races within this
workflow, but does not establish registry-enforced immutability or exclude other
package writers. Before treating the registry as a trusted release source,
independently verify protected source history, exclusive production publisher
permissions, and immutable ordered tags. This change does not modify repository
rules, package access, or registry settings.

Run the local contract tests with `python3 .github/bin/test_trino_release.py`.
These tests mock the registry commands; they do not publish images. After the
first authorized real publication, verify its ordered tag, index annotations,
and revision-alias digest through the registry before enabling deployment
discovery.

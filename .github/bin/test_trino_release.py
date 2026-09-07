#!/usr/bin/env python3

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github/bin/trino-release.sh"
REPOSITORY = "ghcr.io/posthog/trino"
RAW_DIGEST = "sha256:" + "1" * 64
RELEASE_DIGEST = "sha256:" + "2" * 64

DOCKER = r'''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

path = Path(os.environ["MOCK_REGISTRY"])
state = json.loads(path.read_text())
args = sys.argv[1:]
if args[:3] == ["buildx", "imagetools", "inspect"]:
    reference = args[-1]
    if state.get("unavailable"):
        print(state["unavailable"], file=sys.stderr)
        sys.exit(1)
    digest = reference.split("@", 1)[1] if "@" in reference else state["tags"].get(reference)
    if not digest:
        print("manifest unknown", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(state["manifests"][digest] if "--raw" in args else digest))
elif args[:3] == ["buildx", "imagetools", "create"]:
    target = args[args.index("--tag") + 1]
    digest = args[-1].split("@", 1)[1]
    if "--annotation" in args:
        digest = "sha256:" + "2" * 64
        annotations = dict(args[i + 1].removeprefix("index:").split("=", 1)
                           for i, item in enumerate(args) if item == "--annotation")
        state["manifests"][digest] = {"annotations": annotations}
    state["tags"][target] = digest
    state["writes"].append(target)
    path.write_text(json.dumps(state))
else:
    raise RuntimeError(args)
'''


class ReleaseContractTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.registry = self.directory / "registry.json"
        self.registry.write_text(json.dumps({
            "tags": {}, "manifests": {RAW_DIGEST: {
                "mediaType": "application/vnd.oci.image.manifest.v1+json"}}, "writes": []}))
        self.output = self.directory / "output"
        self.output.touch()
        for name, content in {
            "docker": DOCKER,
            "timeout": '#!/bin/sh\nshift\nshift\nexec "$@"\n',
        }.items():
            command = self.directory / name
            command.write_text(content)
            command.chmod(0o755)
        self.sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        count = int(subprocess.check_output(["git", "rev-list", "--first-parent", "--count", "HEAD"], cwd=ROOT, text=True))
        self.tag = f"r{count:012d}-{self.sha[:6]}"
        self.environment = dict(os.environ, PATH=f"{self.directory}:{os.environ['PATH']}",
                                MOCK_REGISTRY=str(self.registry), GITHUB_OUTPUT=str(self.output),
                                GITHUB_REPOSITORY="PostHog/trino", GITHUB_REF="refs/heads/master",
                                GITHUB_EVENT_NAME="push", GITHUB_SHA=self.sha,
                                GITHUB_RUN_ID="12345", GITHUB_RUN_ATTEMPT="1", BUILD_DIGEST=RAW_DIGEST)

    def run_phase(self, phase, success=True):
        result = subprocess.run(["bash", str(SCRIPT), phase], cwd=ROOT,
                                env=self.environment, capture_output=True, text=True)
        self.assertEqual(result.returncode == 0, success, result.stderr)
        return result

    def state(self):
        return json.loads(self.registry.read_text())

    def test_new_release_and_rerun_preserve_digest_and_provenance(self):
        self.run_phase("prepare")
        self.assertIn(f"ordered-tag={self.tag}\ndigest=\n", self.output.read_text())
        self.run_phase("publish")
        state = self.state()
        self.assertEqual(state["tags"][f"{REPOSITORY}:{self.tag}"], RELEASE_DIGEST)
        self.assertEqual(state["tags"][f"{REPOSITORY}:{self.sha}"], RELEASE_DIGEST)
        self.assertEqual(state["manifests"][RELEASE_DIGEST]["annotations"], {
            "org.opencontainers.image.source": "https://github.com/PostHog/trino",
            "org.opencontainers.image.revision": self.sha})
        self.environment["BUILD_DIGEST"] = ""
        self.environment["GITHUB_RUN_ATTEMPT"] = "2"
        self.environment["READABLE_TAG"] = "test-release"
        self.run_phase("prepare")
        self.assertIn(f"digest={RELEASE_DIGEST}", self.output.read_text())
        self.run_phase("publish")
        state = self.state()
        self.assertEqual(state["writes"].count(f"{REPOSITORY}:{self.tag}"), 1)
        self.assertEqual(state["tags"][f"{REPOSITORY}:test-release"], RELEASE_DIGEST)

    def test_rejects_untrusted_refs_events_and_source_mismatch(self):
        for key, value in [("GITHUB_REF", "refs/heads/feature"),
                           ("GITHUB_EVENT_NAME", "pull_request"),
                           ("GITHUB_REPOSITORY", "someone/trino"),
                           ("GITHUB_SHA", "a" * 40)]:
            with self.subTest(key=key):
                original = self.environment[key]
                self.environment[key] = value
                self.run_phase("publish", success=False)
                self.environment[key] = original
        self.assertEqual(self.state()["writes"], [])

    def test_registry_error_is_not_treated_as_absence(self):
        state = self.state()
        state["unavailable"] = "unauthorized: access denied"
        self.registry.write_text(json.dumps(state))
        self.run_phase("prepare", success=False)
        self.run_phase("publish", success=False)
        self.assertEqual(self.state()["writes"], [])

    def test_rejects_existing_release_with_wrong_provenance(self):
        state = self.state()
        state["tags"][f"{REPOSITORY}:{self.tag}"] = RAW_DIGEST
        self.registry.write_text(json.dumps(state))
        self.run_phase("prepare", success=False)
        self.run_phase("publish", success=False)
        self.assertEqual(self.state()["writes"], [])

    def test_readable_alias_cannot_overwrite_an_ordered_release(self):
        for tag in ["r000000000001-abcdef", "a" * 40, "--invalid"]:
            with self.subTest(tag=tag):
                self.environment["READABLE_TAG"] = tag
                self.run_phase("publish", success=False)
        self.assertEqual(self.state()["writes"], [])

    def test_generic_not_found_is_not_release_absence(self):
        state = self.state()
        state["unavailable"] = "docker: command not found"
        self.registry.write_text(json.dumps(state))
        self.run_phase("prepare", success=False)
        self.run_phase("publish", success=False)
        self.assertEqual(self.state()["writes"], [])

    def test_docker_manifest_cannot_silently_drop_index_annotations(self):
        state = self.state()
        state["manifests"][RAW_DIGEST]["mediaType"] = "application/vnd.docker.distribution.manifest.v2+json"
        self.registry.write_text(json.dumps(state))
        self.run_phase("publish", success=False)
        self.assertEqual(self.state()["writes"], [])


class ImageBuildContractTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.script = self.directory / "core/docker/build.sh"
        self.script.parent.mkdir(parents=True)
        self.script.write_text((ROOT / "core/docker/build.sh").read_text())
        self.calls = self.directory / "docker-arguments.json"
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        commands = {
            self.directory / "mvnw": '#!/bin/sh\nprintf "test-version\\n"\n',
            self.bin / "docker": '#!/usr/bin/env python3\nimport json, os, sys\nfrom pathlib import Path\nPath(os.environ["BUILD_ARGUMENTS"]).write_text(json.dumps(sys.argv[1:]))\n',
        }
        for name in ["cp", "tar", "mv", "rm"]:
            commands[self.bin / name] = '#!/bin/sh\nexit 0\n'
        for command, content in commands.items():
            command.write_text(content)
            command.chmod(0o755)
        self.environment = dict(os.environ, PATH=f"{self.bin}:{os.environ['PATH']}",
                                TMPDIR=str(self.directory), BUILD_ARGUMENTS=str(self.calls))

    def build(self, *arguments):
        return subprocess.run(["bash", str(self.script), *arguments], env=self.environment,
                              capture_output=True, text=True)

    def test_oci_publication_uses_registry_exporter(self):
        reference = f"{REPOSITORY}:build-12345-1"
        result = self.build("-a", "arm64", "-x", "-o", reference)
        self.assertEqual(result.returncode, 0, result.stderr)
        arguments = json.loads(self.calls.read_text())
        self.assertEqual(arguments[:2], ["buildx", "build"])
        self.assertEqual(arguments[arguments.index("--output") + 1], "type=registry,oci-mediatypes=true")
        self.assertIn("--provenance=false", arguments)
        self.assertEqual(arguments[arguments.index("--tag") + 1], reference)
        self.assertEqual(arguments[arguments.index("--platform") + 1], "linux/arm64")

    def test_default_local_build_stays_local(self):
        result = self.build("-a", "arm64", "-x")
        self.assertEqual(result.returncode, 0, result.stderr)
        arguments = json.loads(self.calls.read_text())
        self.assertEqual(arguments[0], "build")
        self.assertNotIn("--output", arguments)
        self.assertEqual(arguments[arguments.index("-t") + 1], "trino:test-version-arm64")

    def test_oci_publication_rejects_multiple_architectures_and_local_tests(self):
        for arguments in [("-x", "-o", "test"), ("-a", "arm64", "-o", "test")]:
            with self.subTest(arguments=arguments):
                self.assertNotEqual(self.build(*arguments).returncode, 0)
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()

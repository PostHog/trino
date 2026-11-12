/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hoglake;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.plugin.hoglake.testing.ConnectorTestFixtures;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.TestingConnectorContext;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_TEAM_CREDENTIAL_UNAVAILABLE;
import static io.trino.plugin.hoglake.TestingHoglakeTeamCredentialSource.accessKey;
import static io.trino.plugin.hoglake.TestingHoglakeTeamCredentialSource.secretKey;
import static io.trino.plugin.hoglake.TestingHoglakeTeamCredentialSource.sessionToken;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class TestHoglakeTeamCredentials
{
    private static final String ROOT = TestingHoglakeTeamCredentialSource.TABLE_LOCATION;
    private static final SchemaTableName TABLE = new SchemaTableName("test", "events");
    private static final String DEV_LOCATION = HoglakeDirectStsCredentialSource.tableLocation("s3://example-bucket/millrace/millrace-dev", HoglakeDirectStsCredentialSource.TABLE);
    private static final HoglakeSplitCredential TEAM_4 = new HoglakeSplitCredential(4, ROOT + "team_id=4/", accessKey(4), secretKey(4), sessionToken(4), 1_800_000_000_000L);

    @Test
    void testContainmentMatchesHoglakeVectors()
            throws IOException
    {
        JsonNode cases = vectors().get("containment_cases");
        assertThat(cases).hasSize(14);
        for (JsonNode vector : cases) {
            assertThat(HoglakeSplitCredential.pathStaysUnder(vector.get("path").asText(), vector.get("prefix").asText()))
                    .as(vector.get("path").asText())
                    .isEqualTo(vector.get("under").asBoolean());
        }
    }

    /**
     * hoglake's long prefix cases, under each case's table root: the parser
     * accepts exactly the canonical values, and the builder gives hoglake's
     * prefix for each of them.
     */
    @Test
    void testParserAndPrefixMatchHoglakeVectors()
            throws IOException
    {
        JsonNode cases = vectors().get("prefix_cases");
        assertThat(cases).hasSize(15);
        for (JsonNode vector : cases) {
            assertThat(vector.get("col_type").asText()).isEqualTo("long");
            String root = HoglakeDirectStsCredentialSource.tableLocation(vector.get("data_path").asText(), new SchemaTableName(vector.get("namespace").asText(), vector.get("table").asText()));
            String path = root + "team_id=" + vector.get("value").asText() + "/a.parquet";
            if (vector.has("expected")) {
                assertThat(HoglakeDirectStsCredentialSource.prefix(root, HoglakeDirectStsCredentialSource.teamOf(root, path)))
                        .as(path)
                        .isEqualTo(vector.get("expected").asText());
            }
            else {
                assertErrorCode(() -> HoglakeDirectStsCredentialSource.teamOf(root, path), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
            }
        }
    }

    @Test
    void testParserRefusesPathsOutsideATeamPrefix()
    {
        assertThat(DEV_LOCATION).isEqualTo("s3://example-bucket/millrace/millrace-dev/data/ingest/events/");
        assertThat(HoglakeDirectStsCredentialSource.teamOf(DEV_LOCATION, DEV_LOCATION + "team_id=4/a.parquet")).isEqualTo(4);
        assertThatThrownBy(() -> HoglakeDirectStsCredentialSource.tableLocation("/local/lake", TABLE)).hasMessageContaining("not an s3:// location");
        for (String path : List.of(
                ROOT + "team_id=9223372036854775808/a.parquet",
                ROOT + "team_id=042/a.parquet",
                ROOT + "team_id=4",
                ROOT + "a.parquet",
                "s3://test-bucket/lake/data/test/other/team_id=4/a.parquet",
                "s3://example-bucket/millrace/millrace-dev/data/ingest/events/a.parquet",
                "s3://example-bucket/millrace/millrace-dev/data/ingest/events_v2/team_id=4/a.parquet")) {
            assertErrorCode(() -> HoglakeDirectStsCredentialSource.teamOf(ROOT, path), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
            assertErrorCode(() -> HoglakeDirectStsCredentialSource.teamOf(DEV_LOCATION, path), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
        }
        // The parser names the team; the match refuses what it lets through.
        assertThat(HoglakeDirectStsCredentialSource.teamOf(ROOT, ROOT + "team_id=4//a.parquet")).isEqualTo(4);
        assertErrorCode(() -> split(ROOT + "team_id=4//a.parquet", Optional.empty()).withCredential(TEAM_4), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
        assertErrorCode(() -> split(ROOT + "team_id=4/a b.parquet", Optional.empty()).withCredential(TEAM_4), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
    }

    @Test
    void testCredentialCoversOnlyItsTeamsDataAndDeletionVectors()
    {
        assertErrorCode(() -> split(ROOT + "team_id=42/a.parquet", Optional.empty()).withCredential(TEAM_4), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
        assertErrorCode(() -> split(ROOT + "team_id=4/a.parquet", Optional.of(ROOT + "team_id=42/dv.puffin")).withCredential(TEAM_4), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
        assertErrorCode(() -> split(ROOT + "team_id=4/a.parquet", Optional.of(ROOT + "hoglake-dv-1.puffin")).withCredential(TEAM_4), HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX);
        HoglakeSplit covered = split(ROOT + "team_id=4/a.parquet", Optional.of(ROOT + "team_id=4/dv.puffin")).withCredential(TEAM_4);
        assertThat(covered.credential()).containsSame(TEAM_4);
        assertThat(covered.withRange(0, 10, SplitWeight.standard()).credential()).containsSame(TEAM_4);
        assertThatThrownBy(() -> new HoglakeSplitCredential(4, ROOT + "team_id=4", "a", "b", "c", 0))
                .hasMessageContaining("does not end with /");
    }

    @Test
    void testSecretsAreRedactedButSerialized()
    {
        HoglakeSplit split = split(ROOT + "team_id=4/a.parquet", Optional.empty()).withCredential(TEAM_4);
        List<String> rendered = new ArrayList<>(List.of(split.toString(), TEAM_4.toString()));
        rendered.add(new TestingHoglakeTeamCredentialSource().vend(ConnectorTestFixtures.session(), TABLE, Set.of(4L)).toString());
        rendered.add(message(() -> split(ROOT + "team_id=42/a.parquet", Optional.of(ROOT + "team_id=42/dv.puffin")).withCredential(TEAM_4)));
        rendered.add(message(() -> HoglakeSplitManager.withCredentials(new TestingHoglakeTeamCredentialSource(42L), ConnectorTestFixtures.session(), TABLE, List.of(split(ROOT + "team_id=42/a.parquet", Optional.empty())))));
        assertThat(split.toString()).contains("teamId=4", ROOT + "team_id=4/");
        assertThat(rendered).allSatisfy(text -> assertThat(text).doesNotContain(accessKey(4), secretKey(4), sessionToken(4), secretKey(42), sessionToken(42)));

        JsonCodec<HoglakeSplit> codec = JsonCodec.jsonCodec(HoglakeSplit.class);
        HoglakeSplit copy = codec.fromJson(codec.toJson(split));
        assertThat(copy).isEqualTo(split);
        assertThat(copy.credential().orElseThrow().extraCredentials()).isEqualTo(ImmutableMap.of(
                EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, accessKey(4),
                EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, secretKey(4),
                EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, sessionToken(4)));
    }

    @Test
    void testOneVendPerQueryAndOneCredentialPerTeam()
    {
        TestingHoglakeTeamCredentialSource source = new TestingHoglakeTeamCredentialSource();
        HoglakeSplit first = split(ROOT + "team_id=4/a.parquet", Optional.empty());
        List<HoglakeSplit> splits = HoglakeSplitManager.withCredentials(source, ConnectorTestFixtures.session(), TABLE, List.of(
                first.withRange(0, 5, SplitWeight.standard()),
                first.withRange(5, 5, SplitWeight.standard()),
                split(ROOT + "team_id=4/b.parquet", Optional.of(ROOT + "team_id=4/b.puffin")),
                split(ROOT + "team_id=42/c.parquet", Optional.empty())));
        assertThat(source.vends).containsExactly(Set.of(4L, 42L));
        assertThat(splits.get(1).credential().orElseThrow()).isSameAs(splits.get(0).credential().orElseThrow()).isSameAs(splits.get(2).credential().orElseThrow());
        assertThat(splits.get(3).credential().orElseThrow().accessKeyId()).isEqualTo(accessKey(42));

        assertThat(HoglakeSplitManager.withCredentials(source, ConnectorTestFixtures.session(), TABLE, List.of())).isEmpty();
        assertThat(source.vends).hasSize(1);
        // A source that does not return every team fails planning.
        assertErrorCode(
                () -> HoglakeSplitManager.withCredentials(new TestingHoglakeTeamCredentialSource(42L), ConnectorTestFixtures.session(), TABLE, splits),
                HOGLAKE_TEAM_CREDENTIAL_UNAVAILABLE);
    }

    @Test
    void testBootstrapRefusesEveryFallbackCredential()
    {
        Map<String, String> smoke = Map.of(
                "hoglake.uri", "http://127.0.0.1:9",
                "hoglake.catalog", "millrace-dev",
                "hoglake.s3-credentials.source", "direct-sts",
                "hoglake.s3-credentials.direct-sts.org-id", "test-org");
        for (Map<String, String> fallback : List.of(
                Map.of("s3.iam-role", "arn:aws:iam::123456789012:role/catalog"),
                Map.of("s3.aws-access-key", "catalog-key", "s3.aws-secret-key", "catalog-secret"),
                Map.of("hoglake.s3.access-key", "catalog-key", "hoglake.s3.secret-key", "catalog-secret"),
                Map.of("s3.security-mapping.enabled", "true", "s3.security-mapping.config-file", "/nonexistent/mapping.json"))) {
            assertThatThrownBy(() -> create(ImmutableMap.<String, String>builder().putAll(smoke).putAll(fallback).buildOrThrow()))
                    .hasMessageContaining("cannot be set with hoglake.s3-credentials.source");
        }
        assertThatThrownBy(() -> create(Map.of("hoglake.uri", "http://127.0.0.1:9", "hoglake.s3-credentials.source", "hogtower")))
                .hasMessageContaining("requires plan 2.8");
        assertThatThrownBy(() -> create(Map.of("hoglake.uri", "http://127.0.0.1:9", "hoglake.s3-credentials.source", "direct-sts")))
                .hasStackTraceContaining("hoglake.s3-credentials.direct-sts.org-id is required");
        // direct-sts serves only the millrace-dev catalog.
        assertThatThrownBy(() -> create(Map.of(
                "hoglake.uri", "http://127.0.0.1:9",
                "hoglake.catalog", "lake",
                "hoglake.s3-credentials.source", "direct-sts",
                "hoglake.s3-credentials.direct-sts.org-id", "test-org")))
                .hasStackTraceContaining("serves only the millrace-dev catalog, not lake");
        // The smoke catalog itself starts and stops without calling AWS.
        create(smoke).shutdown();
    }

    private static Connector create(Map<String, String> config)
    {
        return new HoglakeConnectorFactory().create("smoke", config, new TestingConnectorContext());
    }

    static HoglakeSplit split(String path, Optional<String> deletionVector)
    {
        return new HoglakeSplit(1, path, 10, 2, deletionVector, deletionVector.isPresent() ? 1 : 0, Optional.empty(), 0, 10, SplitWeight.standard(), OptionalLong.empty(), Optional.empty(), Optional.empty());
    }

    static void assertErrorCode(ThrowingCallable call, HoglakeErrorCode errorCode)
    {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(errorCode.toErrorCode()));
    }

    private static String message(ThrowingCallable call)
    {
        return catchThrowableOfType(TrinoException.class, call).getMessage();
    }

    private static JsonNode vectors()
            throws IOException
    {
        try (InputStream input = TestHoglakeTeamCredentials.class.getResourceAsStream("/object-prefix-vectors.json")) {
            return new ObjectMapper().readTree(input);
        }
    }
}

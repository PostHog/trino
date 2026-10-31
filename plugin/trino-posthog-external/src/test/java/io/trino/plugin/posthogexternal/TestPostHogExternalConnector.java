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
package io.trino.plugin.posthogexternal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code lake} catalog holds keys for a MinIO user that can read only {@code tenant-bucket}.
 * The {@code other_keys} catalog shares the same PostHog metastore but holds keys for a user that can read only {@code other-bucket}.
 */
final class TestPostHogExternalConnector
        extends AbstractTestQueryFramework
{
    private static final Path PERSON_TABLE = Path.of("../trino-delta-lake/src/test/resources/databricks73/person");
    private static final String BUCKET = "tenant-bucket";
    private static final String CATALOG_ID = "cat_test";
    private static final String TOKEN = "service-token";

    private Minio minio;
    private FakePostHogMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = Minio.builder().build();
        minio.start();
        minio.createBucket(BUCKET);
        minio.createBucket("other-bucket");
        try (S3Client root = s3Client()) {
            for (String prefix : ImmutableList.of("person", "dropped", "writes", "other_keys")) {
                upload(root, prefix);
            }
        }
        createUserReadingOnly("tenant", "tenant-secret", BUCKET);
        createUserReadingOnly("other", "other-secret", "other-bucket");

        metastore = new FakePostHogMetastore(CATALOG_ID, TOKEN);
        Path tokenFile = Files.createTempFile("metastore-token", "");
        Files.writeString(tokenFile, TOKEN + "\n");

        Session session = testSessionBuilder().setCatalog("lake").setSchema("billing").build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        queryRunner.installPlugin(new PostHogExternalPlugin());
        queryRunner.createCatalog("lake", PostHogExternalConnectorFactory.CONNECTOR_NAME, catalogProperties(tokenFile, "tenant", "tenant-secret"));
        queryRunner.createCatalog("other_keys", PostHogExternalConnectorFactory.CONNECTOR_NAME, catalogProperties(tokenFile, "other", "other-secret"));
        queryRunner.execute("CREATE SCHEMA lake.billing");
        queryRunner.execute("CALL lake.system.register_table(schema_name => 'billing', table_name => 'writes', table_location => 's3://" + BUCKET + "/writes')");
        return queryRunner;
    }

    private Map<String, String> catalogProperties(Path tokenFile, String accessKey, String secretKey)
    {
        return ImmutableMap.<String, String>builder()
                .put("posthog-external.metastore-uri", metastore.uri().toString())
                .put("posthog-external.metastore-token-file", tokenFile.toString())
                .put("posthog-external.catalog-id", CATALOG_ID)
                .put("fs.native-s3.enabled", "true")
                .put("s3.endpoint", minio.getMinioAddress())
                .put("s3.region", MINIO_REGION)
                .put("s3.path-style-access", "true")
                .put("s3.aws-access-key", accessKey)
                .put("s3.aws-secret-key", secretKey)
                .put("fs.cache.enabled", "false")
                .buildOrThrow();
    }

    @AfterAll
    void stop()
    {
        if (metastore != null) {
            metastore.close();
        }
        if (minio != null) {
            minio.close();
        }
    }

    @Test
    void testRegisterReadAndUnregister()
    {
        register("billing", "person", "person");
        assertThat(metastore.table("billing", "person").orElseThrow().path("location").asText()).isEqualTo("s3://" + BUCKET + "/person");
        assertThat(metastore.table("billing", "person").orElseThrow().path("columns").isEmpty()).isTrue();

        assertQuery("SELECT count(*) FROM lake.billing.person", "VALUES 12");
        assertQuery("SELECT name, age FROM lake.billing.person WHERE name = 'Bob'", "VALUES ('Bob', 21)");
        assertThat(computeActual("SHOW TABLES FROM lake.billing").getOnlyColumnAsSet()).contains("person");

        assertUpdate("CALL lake.system.unregister_table(schema_name => 'billing', table_name => 'person')");
        assertThat(metastore.table("billing", "person")).isEmpty();
        assertQueryFails("SELECT count(*) FROM lake.billing.person", ".*Table 'lake.billing.person' does not exist");
        assertThat(objectsUnder("person")).isNotEmpty();
    }

    @Test
    void testDropTableRemovesOnlyTheRegistration()
    {
        register("billing", "dropped", "dropped");
        Map<String, String> before = objectsUnder("dropped");

        assertUpdate("DROP TABLE lake.billing.dropped");

        assertThat(metastore.table("billing", "dropped")).isEmpty();
        assertThat(objectsUnder("dropped")).isEqualTo(before);
    }

    @Test
    void testRegistrationIsVisibleToTheNextQuery()
    {
        assertQueryReturnsEmptyResult("SHOW TABLES FROM lake.billing LIKE 'fresh'");
        register("billing", "fresh", "person");
        assertQuery("SELECT count(*) FROM lake.billing.fresh", "VALUES 12");
        assertUpdate("CALL lake.system.unregister_table(schema_name => 'billing', table_name => 'fresh')");
        assertQueryReturnsEmptyResult("SHOW TABLES FROM lake.billing LIKE 'fresh'");
    }

    @Test
    void testSchemaLifecycle()
    {
        assertUpdate("CREATE SCHEMA lake.scratch");
        assertThat(computeActual("SHOW SCHEMAS FROM lake").getOnlyColumnAsSet()).contains("scratch");
        assertQueryFails("CREATE SCHEMA lake.scratch", ".*Schema 'lake.scratch' already exists");
        assertQueryFails("CREATE SCHEMA lake.located WITH (location = 's3://" + BUCKET + "/located')", ".*cannot have a location");
        register("scratch", "person", "person");
        assertQueryFails("DROP SCHEMA lake.scratch", ".*Cannot drop non-empty schema 'scratch'");
        assertUpdate("CALL lake.system.unregister_table(schema_name => 'scratch', table_name => 'person')");
        assertUpdate("DROP SCHEMA lake.scratch");
        assertThat(computeActual("SHOW SCHEMAS FROM lake").getOnlyColumnAsSet()).doesNotContain("scratch");
    }

    @Test
    void testDropSchemaCascadeRemovesOnlyRegistrations()
    {
        assertUpdate("CREATE SCHEMA lake.cascaded");
        register("cascaded", "person", "person");
        Map<String, String> before = objectsUnder("person");

        assertUpdate("DROP SCHEMA lake.cascaded CASCADE");

        assertThat(computeActual("SHOW SCHEMAS FROM lake").getOnlyColumnAsSet()).doesNotContain("cascaded");
        assertThat(metastore.table("cascaded", "person")).isEmpty();
        assertThat(objectsUnder("person")).isEqualTo(before);
    }

    @Test
    void testKeysThatCannotReadTheBucketAreRefused()
    {
        register("billing", "other_keys", "other_keys");
        assertThatThrownBy(() -> computeActual("SELECT count(*) FROM other_keys.billing.other_keys"))
                .hasStackTraceContaining("AccessDenied");
        assertThatThrownBy(() -> computeActual("CALL other_keys.system.register_table(schema_name => 'billing', table_name => 'denied', table_location => 's3://" + BUCKET + "/person')"))
                .hasStackTraceContaining("AccessDenied");
        assertThat(metastore.table("billing", "denied")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "INSERT INTO lake.billing.writes (name, age) VALUES ('x', 1)",
            "UPDATE lake.billing.writes SET name = 'x'",
            "DELETE FROM lake.billing.writes WHERE age = 42",
            "TRUNCATE TABLE lake.billing.writes",
            "MERGE INTO lake.billing.writes t USING (VALUES 'Bob') s(name) ON t.name = s.name WHEN MATCHED THEN DELETE",
            "CREATE TABLE lake.billing.created (x bigint) WITH (location = 's3://" + BUCKET + "/created')",
            "CREATE TABLE lake.billing.ctas WITH (location = 's3://" + BUCKET + "/ctas') AS SELECT 1 x",
            "CREATE TABLE lake.billing.managed (x bigint)",
            "ALTER TABLE lake.billing.writes ADD COLUMN extra bigint",
            "ALTER TABLE lake.billing.writes RENAME TO renamed",
            "ALTER TABLE lake.billing.writes SET PROPERTIES change_data_feed_enabled = true",
            "COMMENT ON TABLE lake.billing.writes IS 'x'",
            "ALTER TABLE lake.billing.writes EXECUTE optimize",
            "ANALYZE lake.billing.writes",
            "CALL lake.system.vacuum('billing', 'writes', '7d')",
            "CALL lake.system.drop_extended_stats('billing', 'writes')",
            "CREATE VIEW lake.billing.v AS SELECT 1 x",
    })
    void testWritesAreRefused(String statement)
    {
        Map<String, String> before = objectsUnder("writes");

        assertThatThrownBy(() -> computeActual(statement))
                .hasMessageMatching("(?s).*(Access Denied: Cannot|Tables in this catalog are read-only).*");

        assertThat(objectsUnder("writes")).isEqualTo(before);
        assertThat(objectsUnder("created")).isEmpty();
        assertThat(objectsUnder("ctas")).isEmpty();
        assertThat(metastore.table("billing", "created")).isEmpty();
        assertThat(metastore.table("billing", "ctas")).isEmpty();
        assertThat(metastore.table("billing", "managed")).isEmpty();
        assertThat(metastore.table("billing", "v")).isEmpty();
        assertThat(metastore.table("billing", "writes")).isPresent();
        assertQuery("SELECT count(*) FROM lake.billing.writes", "VALUES 12");
    }

    @Test
    void testAuditHeaderNamesTheSessionUser()
    {
        assertThat(metastore.requests()).isNotEmpty();
        assertThat(metastore.requests()).anySatisfy(request -> assertThat(request.user()).contains(getSession().getUser()));
    }

    private void register(String schema, String table, String prefix)
    {
        assertUpdate(format(
                "CALL lake.system.register_table(schema_name => '%s', table_name => '%s', table_location => 's3://%s/%s')",
                schema,
                table,
                BUCKET,
                prefix));
    }

    private Map<String, String> objectsUnder(String prefix)
    {
        try (S3Client root = s3Client()) {
            List<S3Object> objects = root.listObjectsV2Paginator(builder -> builder.bucket(BUCKET).prefix(prefix + "/")).contents().stream().toList();
            return objects.stream().collect(toImmutableMap(S3Object::key, S3Object::eTag));
        }
    }

    private void createUserReadingOnly(String user, String secret, String bucket)
    {
        String policy = format(
                """
                {"Version": "2012-10-17", "Statement": [
                  {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
                   "Resource": ["arn:aws:s3:::%1$s", "arn:aws:s3:::%1$s/*"]}
                ]}""",
                bucket);
        minio.executeInContainerFailOnError("sh", "-c", String.join(
                " && ",
                format("mc alias set local http://localhost:%s %s %s >/dev/null", Minio.MINIO_API_PORT, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD),
                format("printf '%%s' '%s' > /tmp/%s.json", policy.replace("\n", " "), user),
                format("mc admin policy create local read-%s /tmp/%s.json", bucket, user),
                format("mc admin user add local %s %s", user, secret),
                format("mc admin policy attach local read-%s --user %s", bucket, user)));
    }

    private S3Client s3Client()
    {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getMinioAddress()))
                .region(Region.of(MINIO_REGION))
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)))
                .build();
    }

    private static void upload(S3Client client, String prefix)
            throws IOException
    {
        try (Stream<Path> files = Files.walk(PERSON_TABLE)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String key = prefix + "/" + PERSON_TABLE.relativize(file).toString().replace('\\', '/');
                client.putObject(builder -> builder.bucket(BUCKET).key(key), file);
            }
        }
    }
}

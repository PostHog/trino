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

import com.google.common.base.Suppliers;
import io.trino.filesystem.Location;
import io.trino.spi.Node;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.Tag;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX;
import static io.trino.plugin.hoglake.HoglakeErrorCode.HOGLAKE_TEAM_CREDENTIAL_UNAVAILABLE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Dev-only smoke source (plan 2.9s), removed once the {@code hogtower} source
 * works in dev. It serves only the {@code millrace-dev} catalog's
 * {@code ingest.events}: the coordinator assumes {@code hoglake-events-reader-dev}
 * in its own AWS account, once per team, with the team and org session tags
 * and a session policy for the team's literal prefix. The table location
 * comes from the catalog's {@code data_path}. No property names a role or a
 * location.
 */
final class HoglakeDirectStsCredentialSource
        implements HoglakeTeamCredentialSource
{
    static final String CATALOG = "millrace-dev";
    static final SchemaTableName TABLE = new SchemaTableName("ingest", "events");
    static final String ROLE_NAME = "hoglake-events-reader-dev";
    static final String REGION = "us-east-1";
    // C′'s cache rule: serve an entry only while this much of its lifetime remains.
    static final Duration MINIMUM_REMAINING = Duration.ofMinutes(30);
    private static final Pattern TEAM_SEGMENT = Pattern.compile("team_id=(0|[1-9][0-9]{0,18})/.*", Pattern.DOTALL);

    private final String orgId;
    private final Node currentNode;
    private final StsClient sts;
    private final Supplier<String> tableLocation;
    private final Supplier<String> roleArn;
    private final Map<Long, TeamCredential> cache = new ConcurrentHashMap<>();

    HoglakeDirectStsCredentialSource(String catalog, String orgId, Node currentNode, Supplier<String> dataPath)
    {
        this(catalog, orgId, currentNode, dataPath, StsClient.builder().region(Region.of(REGION)).credentialsProvider(DefaultCredentialsProvider.builder().build()).build());
    }

    HoglakeDirectStsCredentialSource(String catalog, String orgId, Node currentNode, Supplier<String> dataPath, StsClient sts)
    {
        checkArgument(catalog.equals(CATALOG), "hoglake.s3-credentials.source=direct-sts serves only the %s catalog, not %s", CATALOG, catalog);
        this.orgId = requireNonNull(orgId, "orgId is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.sts = requireNonNull(sts, "sts is null");
        requireNonNull(dataPath, "dataPath is null");
        // Both are resolved on the first use, on the coordinator: workers have no AWS identity.
        this.tableLocation = Suppliers.memoize(() -> tableLocation(dataPath.get(), TABLE));
        this.roleArn = Suppliers.memoize(() -> "arn:aws:iam::%s:role/%s".formatted(sts.getCallerIdentity().account(), ROLE_NAME));
    }

    @Override
    public TeamCredentials vend(ConnectorSession session, SchemaTableName table, Set<Long> teamIds)
    {
        if (!currentNode.isCoordinator()) {
            throw new IllegalStateException("Team credentials are vended on the coordinator only");
        }
        String location = tableLocation(table);
        Instant servableUntil = Instant.now().plus(MINIMUM_REMAINING);
        Map<Long, TeamCredential> credentials = new HashMap<>();
        for (long team : teamIds) {
            TeamCredential credential = cache.get(team);
            if (credential == null || credential.expiration().isBefore(servableUntil)) {
                credential = mint(location, team);
                cache.put(team, credential);
            }
            credentials.put(team, credential);
        }
        return new TeamCredentials(REGION, Location.of(location).host().orElseThrow(), credentials);
    }

    @Override
    public long teamOf(SchemaTableName table, String dataPath)
    {
        return teamOf(tableLocation(table), dataPath);
    }

    @Override
    public void close()
    {
        sts.close();
    }

    private String tableLocation(SchemaTableName table)
    {
        if (!table.equals(TABLE)) {
            throw new TrinoException(NOT_SUPPORTED, "hoglake.s3-credentials.source=direct-sts reads only %s %s, not %s".formatted(CATALOG, TABLE, table));
        }
        return tableLocation.get();
    }

    private TeamCredential mint(String location, long team)
    {
        String prefix = prefix(location, team);
        Credentials credentials;
        try {
            credentials = sts.assumeRole(AssumeRoleRequest.builder()
                            .roleArn(roleArn.get())
                            .roleSessionName(sessionName(team, orgId))
                            .durationSeconds(3600)
                            .tags(Tag.builder().key("hoglake-team-id").value(Long.toString(team)).build(), Tag.builder().key("hoglake-org-id").value(orgId).build())
                            .policy(policy(prefix))
                            .build())
                    .credentials();
        }
        catch (SdkException e) {
            // The error code only, never the request.
            String code = e instanceof AwsServiceException service && service.awsErrorDetails() != null ? service.awsErrorDetails().errorCode() : e.getClass().getSimpleName();
            throw new TrinoException(HOGLAKE_TEAM_CREDENTIAL_UNAVAILABLE, "AssumeRole for team %d failed: %s".formatted(team, code));
        }
        String keyPrefix = prefix.substring(("s3://" + Location.of(prefix).host().orElseThrow() + "/").length());
        return new TeamCredential(keyPrefix, credentials.accessKeyId(), credentials.secretAccessKey(), credentials.sessionToken(), credentials.expiration());
    }

    /**
     * {@code <data_path>/data/<namespace>/<table>/}, the root of hoglake's
     * object-prefix layout.
     */
    static String tableLocation(String dataPath, SchemaTableName table)
    {
        Location location = Location.of(dataPath);
        checkArgument(location.scheme().equals(Optional.of("s3")) && location.host().isPresent(), "data_path is not an s3:// location: %s", dataPath);
        String root = dataPath.endsWith("/") ? dataPath.substring(0, dataPath.length() - 1) : dataPath;
        return "%s/data/%s/%s/".formatted(root, table.getSchemaName(), table.getTableName());
    }

    static String prefix(String tableLocation, long team)
    {
        checkArgument(team >= 0, "team is negative: %s", team);
        return tableLocation + "team_id=" + team + "/";
    }

    static String policy(String prefix)
    {
        return "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::%s*\"}]}"
                .formatted(prefix.substring("s3://".length()));
    }

    static String sessionName(long team, String orgId)
    {
        String name = "hoglake-%d-%s-%s".formatted(team, orgId, HexFormat.of().toHexDigits(ThreadLocalRandom.current().nextInt()));
        name = name.replaceAll("[^\\w+=,.@-]", "-");
        return name.substring(0, Math.min(name.length(), 64));
    }

    /**
     * Smoke only: the team is the canonical decimal after
     * {@code <table location>team_id=} and before the next {@code /}.
     * Every other path fails planning; the prefix match checks the rest.
     */
    static long teamOf(String tableLocation, String dataPath)
    {
        Matcher matcher = TEAM_SEGMENT.matcher(dataPath);
        if (dataPath.startsWith(tableLocation) && matcher.region(tableLocation.length(), dataPath.length()).matches()) {
            try {
                return Long.parseLong(matcher.group(1));
            }
            catch (NumberFormatException _) {
                // Above Long.MAX_VALUE.
            }
        }
        throw new TrinoException(HOGLAKE_PATH_OUTSIDE_TEAM_PREFIX, "%s is not under a team prefix of %s".formatted(dataPath, tableLocation));
    }
}

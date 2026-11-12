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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Vends {@code ASIA-TEAM-<id>} for each team of a test table, parsing the
 * team from the path exactly as the smoke's {@code direct-sts} source does,
 * and records every vend.
 */
final class TestingHoglakeTeamCredentialSource
        implements HoglakeTeamCredentialSource
{
    static final String BUCKET = "test-bucket";
    static final String TABLE_LOCATION = "s3://test-bucket/lake/data/test/events/";

    final List<Set<Long>> vends = new CopyOnWriteArrayList<>();
    private final Set<Long> withheld;

    TestingHoglakeTeamCredentialSource(Long... withheld)
    {
        this.withheld = Set.of(withheld);
    }

    static String accessKey(long team)
    {
        return "ASIA-TEAM-" + team;
    }

    static String secretKey(long team)
    {
        return "secret-of-team-" + team;
    }

    static String sessionToken(long team)
    {
        return "token-of-team-" + team;
    }

    @Override
    public TeamCredentials vend(ConnectorSession session, SchemaTableName table, Set<Long> teamIds)
    {
        vends.add(teamIds);
        Map<Long, TeamCredential> credentials = new HashMap<>();
        for (long team : teamIds) {
            if (!withheld.contains(team)) {
                String prefix = HoglakeDirectStsCredentialSource.prefix(TABLE_LOCATION, team).substring(("s3://" + BUCKET + "/").length());
                credentials.put(team, new TeamCredential(prefix, accessKey(team), secretKey(team), sessionToken(team), Instant.now().plus(Duration.ofHours(1))));
            }
        }
        return new TeamCredentials("us-east-1", BUCKET, credentials);
    }

    @Override
    public long teamOf(SchemaTableName table, String dataPath)
    {
        return HoglakeDirectStsCredentialSource.teamOf(TABLE_LOCATION, dataPath);
    }
}

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

import java.time.Instant;
import java.util.Map;
import java.util.Set;

/**
 * Vends one S3 read credential per team, on the coordinator only. Only the
 * split manager calls it; workers get each credential inside the splits.
 */
public interface HoglakeTeamCredentialSource
        extends AutoCloseable
{
    // A credential for every requested team of the table, or a throw that fails the query.
    TeamCredentials vend(ConnectorSession session, SchemaTableName table, Set<Long> teamIds);

    // The team whose prefix holds a data file. Smoke only: parsed from the path until the scan carries it (plan 1.3).
    long teamOf(SchemaTableName table, String dataPath);

    @Override
    default void close() {}

    record TeamCredentials(String region, String bucket, Map<Long, TeamCredential> credentials) {}

    // prefix is a key prefix in the bucket, ending in '/'.
    record TeamCredential(String prefix, String accessKeyId, String secretAccessKey, String sessionToken, Instant expiration)
    {
        @Override
        public String toString()
        {
            return "TeamCredential[prefix=%s, expiration=%s]".formatted(prefix, expiration);
        }
    }
}

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

import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * One metastore per query identity, so each call can name the session user for the audit header.
 */
public class PostHogExternalMetastoreFactory
        implements HiveMetastoreFactory
{
    private final PostHogMetastoreClient client;

    public PostHogExternalMetastoreFactory(PostHogMetastoreClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    @Override
    public HiveMetastore createMetastore(Optional<ConnectorIdentity> identity)
    {
        return new PostHogExternalMetastore(client, identity.map(ConnectorIdentity::getUser));
    }
}

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

import io.trino.plugin.hoglake.rest.HoglakeClient;

import java.io.Closeable;
import java.util.Locale;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * The read-only schema {@value #SCHEMA_NAME}: the org's events mount, the Hoglake
 * catalog {@code <orgID>-events} on the same server as the org catalog
 * {@code <orgID>}, read with the org catalog's own token.
 *
 * <p>With it configured, the name {@value #SCHEMA_NAME} always means the mount: a
 * namespace of that name in the org's own catalog is never listed, read or written,
 * whether or not the mount exists or answers. Falling back to the org's namespace
 * would let a mount outage put user-writable data under the name of PostHog's data.
 */
public final class HoglakePosthogSchema
        implements Closeable
{
    public static final String SCHEMA_NAME = "posthog";

    // hoglake MountNames.PATTERN without the suffix: a canonical lowercase UUID.
    private static final Pattern ORG_ID = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    private static final String MOUNT_SUFFIX = "-events";

    private final String mountCatalog;
    private final HoglakeClient client;

    public HoglakePosthogSchema(String orgCatalog, HoglakeClient client)
    {
        if (!isOrgCatalog(orgCatalog)) {
            throw new IllegalArgumentException("The posthog schema requires an org catalog named by a lowercase UUID");
        }
        this.mountCatalog = mountCatalog(orgCatalog);
        this.client = requireNonNull(client, "client is null");
    }

    public static boolean isOrgCatalog(String catalog)
    {
        return catalog != null && ORG_ID.matcher(catalog).matches();
    }

    public static String mountCatalog(String orgCatalog)
    {
        return orgCatalog + MOUNT_SUFFIX;
    }

    /**
     * Whether a Trino schema name addresses this schema. Hoglake namespace names are
     * compared without case, so no spelling of an org namespace can stand beside it.
     */
    public static boolean isPosthogSchema(String schemaName)
    {
        return schemaName != null && schemaName.toLowerCase(Locale.ROOT).equals(SCHEMA_NAME);
    }

    public String mountCatalog()
    {
        return mountCatalog;
    }

    public HoglakeClient client()
    {
        return client;
    }

    @Override
    public void close()
    {
        client.close();
    }
}

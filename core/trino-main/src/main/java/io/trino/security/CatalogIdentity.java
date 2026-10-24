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
package io.trino.security;

import io.trino.spi.catalog.CatalogName;

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class CatalogIdentity
{
    private static final String RESERVED_PREFIX = "external_identity";
    private static final Pattern QUALIFIED_GROUP = Pattern.compile("external_identity_[0-9a-f]{32}_[0-9a-f]{32}");

    private CatalogIdentity() {}

    public static void validateBinding(CatalogName catalogName, String group)
    {
        checkArgument(!group.isBlank(), "Catalog identity group is empty");
        if (group.startsWith(RESERVED_PREFIX)) {
            checkArgument(isNameClaim(catalogName, group), "Catalog identity group does not match the catalog name");
        }
    }

    /**
     * A member group widens who satisfies a catalog's identity binding, so it is honoured only
     * beside a required identity group, and it can never itself be an identity group: naming one
     * would let a definition borrow another catalog's name claim.
     */
    public static void validateMemberGroup(Optional<String> requiredIdentityGroup, Optional<String> memberGroup)
    {
        memberGroup.ifPresent(group -> {
            checkArgument(requiredIdentityGroup.isPresent(), "Catalog identity member group requires a catalog identity group");
            checkArgument(!group.isBlank(), "Catalog identity member group is empty");
            checkArgument(!group.startsWith(RESERVED_PREFIX), "Catalog identity member group must not be an identity group");
        });
    }

    // Only the trusted identity groups supply a catalog name claim.
    public static boolean identityClaimsName(CatalogName catalogName, Set<String> groups)
    {
        String prefix = namePrefix(catalogName);
        return groups.stream().anyMatch(group -> group.startsWith(prefix) && QUALIFIED_GROUP.matcher(group).matches());
    }

    private static boolean isNameClaim(CatalogName catalogName, String group)
    {
        return group.startsWith(namePrefix(catalogName)) && QUALIFIED_GROUP.matcher(group).matches();
    }

    private static String namePrefix(CatalogName catalogName)
    {
        return RESERVED_PREFIX + "_" + sha256().hashString(catalogName.toString(), UTF_8).toString().substring(0, 32) + "_";
    }
}

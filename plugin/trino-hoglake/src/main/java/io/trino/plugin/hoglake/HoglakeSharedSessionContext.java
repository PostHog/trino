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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

final class HoglakeSharedSessionContext
{
    private final String catalog;
    private final String token;
    private final String principal;
    private final String user;
    private final Set<String> scopes;
    private final Map<String, Predicate> predicates;
    private final Placement placement;

    HoglakeSharedSessionContext(String catalog, String token, String principal, String user, Set<String> scopes, Map<String, Predicate> predicates, Placement placement)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.token = requireNonNull(token, "token is null");
        this.principal = requireNonNull(principal, "principal is null");
        this.user = requireNonNull(user, "user is null");
        this.scopes = ImmutableSet.copyOf(scopes);
        this.predicates = ImmutableMap.copyOf(predicates);
        this.placement = requireNonNull(placement, "placement is null");
    }

    public String catalog()
    {
        return catalog;
    }

    public String token()
    {
        return token;
    }

    public String principal()
    {
        return principal;
    }

    public String user()
    {
        return user;
    }

    public Set<String> scopes()
    {
        return scopes;
    }

    public Map<String, Predicate> predicates()
    {
        return predicates;
    }

    public Placement placement()
    {
        return placement;
    }

    @Override
    public String toString()
    {
        return "HoglakeSharedSessionContext{redacted}";
    }

    record Predicate(Set<String> read, Set<String> write)
    {
        Predicate
        {
            read = ImmutableSet.copyOf(read);
            write = ImmutableSet.copyOf(write);
        }

        @Override
        public String toString()
        {
            return "Predicate{redacted}";
        }
    }

    record Placement(UUID backendId, String kind, Optional<String> tenant)
    {
        Placement
        {
            requireNonNull(backendId, "backendId is null");
            requireNonNull(kind, "kind is null");
            requireNonNull(tenant, "tenant is null");
        }

        @Override
        public String toString()
        {
            return "Placement{redacted}";
        }
    }
}

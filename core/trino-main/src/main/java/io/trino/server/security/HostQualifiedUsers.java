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
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Qualifies a password login with the tenant named by the request host.
 *
 * <p>A multi-tenant deployment gives each tenant its own host name,
 * {@code <tenant>.<domain>}, while a password file is one flat namespace.
 * With a configured domain, a login as {@code alice} to
 * {@code tenant-a.<domain>} authenticates as {@code tenant-a.alice}, so two
 * tenants can each have an {@code alice} without either name colliding.
 *
 * <p>Qualification only selects which password entry the login is checked
 * against; the password is still verified. A client that sends a different
 * host therefore gains nothing it could not get by typing the qualified name
 * itself against a host outside the domain.
 *
 * <p>A host that is not exactly one DNS label under a configured domain, or
 * whose label is excluded, leaves the user unchanged. Excluded labels keep
 * operational host names under the same domain from being read as tenants.
 */
public final class HostQualifiedUsers
{
    private static final Pattern LABEL = Pattern.compile("[a-z0-9]([a-z0-9-]*[a-z0-9])?");
    private static final String SEPARATOR = ".";

    private final List<String> domainSuffixes;
    private final Set<String> excludedLabels;

    public HostQualifiedUsers(Collection<String> domains, Collection<String> excludedLabels)
    {
        this.domainSuffixes = domains.stream()
                .map(HostQualifiedUsers::normalizeDomain)
                .map(domain -> SEPARATOR + domain)
                .collect(toImmutableList());
        this.excludedLabels = excludedLabels.stream()
                .map(label -> label.toLowerCase(ENGLISH))
                .collect(toImmutableSet());
    }

    public static HostQualifiedUsers disabled()
    {
        return new HostQualifiedUsers(ImmutableList.of(), ImmutableSet.of());
    }

    public boolean isEnabled()
    {
        return !domainSuffixes.isEmpty();
    }

    /**
     * Returns the name to authenticate {@code user} as when the request was
     * addressed to {@code host}.
     */
    public String qualify(String user, Optional<String> host)
    {
        requireNonNull(user, "user is null");
        return tenant(host)
                .map(tenant -> tenant + SEPARATOR + user)
                .orElse(user);
    }

    Optional<String> tenant(Optional<String> host)
    {
        if (host.isEmpty() || domainSuffixes.isEmpty()) {
            return Optional.empty();
        }
        String normalized = stripTrailingDot(host.get().toLowerCase(ENGLISH));
        for (String suffix : domainSuffixes) {
            if (!normalized.endsWith(suffix)) {
                continue;
            }
            String label = normalized.substring(0, normalized.length() - suffix.length());
            // A nested domain can also be configured, so a label that is not a tenant under this
            // suffix may still be one under a longer suffix.
            if (LABEL.matcher(label).matches() && !excludedLabels.contains(label)) {
                return Optional.of(label);
            }
        }
        return Optional.empty();
    }

    private static String normalizeDomain(String domain)
    {
        String normalized = stripTrailingDot(domain.trim().toLowerCase(ENGLISH));
        if (normalized.startsWith(SEPARATOR)) {
            normalized = normalized.substring(1);
        }
        checkArgument(!normalized.isEmpty(), "Host-qualified user domain is empty");
        for (String label : normalized.split("\\.", -1)) {
            checkArgument(LABEL.matcher(label).matches(), "Invalid host-qualified user domain: %s", domain);
        }
        return normalized;
    }

    private static String stripTrailingDot(String value)
    {
        if (value.endsWith(SEPARATOR)) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }
}

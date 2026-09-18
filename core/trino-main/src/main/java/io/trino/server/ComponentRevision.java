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
package io.trino.server;

import io.trino.spi.security.LoadedConfiguration;

import static java.util.Objects.requireNonNull;

/**
 * What one loaded security component reports about the configuration data it currently answers
 * with. A component that does not implement {@link LoadedConfiguration} leaves {@code revision}
 * empty: its data is not acknowledged by this process, and a controller must not treat a missing
 * revision as agreement.
 *
 * @param kind which part of the security configuration this is, for example {@code
 *         password-authenticator}
 * @param name the configured implementation, for example {@code file}
 * @param revision the fingerprint of the data in effect, or null if the component cannot report one
 * @param error why the revision is missing, or null
 */
public record ComponentRevision(String kind, String name, String revision, String error)
{
    public ComponentRevision
    {
        requireNonNull(kind, "kind is null");
        requireNonNull(name, "name is null");
    }

    public static ComponentRevision of(String kind, String name, Object component)
    {
        if (!(component instanceof LoadedConfiguration loadedConfiguration)) {
            return new ComponentRevision(kind, name, null, "%s '%s' does not report the configuration it has loaded".formatted(kind, name));
        }
        try {
            return new ComponentRevision(kind, name, requireNonNull(loadedConfiguration.loadedRevision(), "loadedRevision is null"), null);
        }
        catch (RuntimeException e) {
            // An unreadable state is reported as such; it must never look like an acknowledged revision
            return new ComponentRevision(kind, name, null, e.getMessage() == null ? e.toString() : e.getMessage());
        }
    }
}

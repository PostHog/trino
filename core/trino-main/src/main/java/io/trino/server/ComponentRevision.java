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

import io.airlift.log.Logger;
import io.trino.connector.CatalogSyncFailure;
import io.trino.spi.security.LoadedConfiguration;

import static io.trino.connector.CatalogSyncFailure.COMPONENT_DOES_NOT_REPORT;
import static io.trino.connector.CatalogSyncFailure.COMPONENT_NOT_CONFIGURED;
import static io.trino.connector.CatalogSyncFailure.COMPONENT_UNAVAILABLE;
import static io.trino.server.FailureSummary.summarize;
import static java.util.Objects.requireNonNull;

/**
 * What one loaded security component reports about the configuration data it currently answers
 * with. A component that does not implement {@link LoadedConfiguration}, or that cannot read its
 * data right now, leaves {@code revision} empty: its data is not acknowledged by this process, and
 * a controller must not treat a missing revision as agreement.
 *
 * <p>{@code error} is one of the {@link CatalogSyncFailure} categories and never the text of the
 * underlying failure. A password store names the file it could not read and an authorization
 * backend names its endpoint; readiness is readable with management-read authorization, which does
 * not authorize reading either of those. The failure itself goes to the server log.
 *
 * @param kind which part of the security configuration this is, for example {@code
 *         password-authenticator}
 * @param name the configured implementation, for example {@code file}
 * @param revision the fingerprint of the data in effect, or null if the component cannot report one
 * @param error the category explaining a missing revision, or null
 */
public record ComponentRevision(String kind, String name, String revision, String error)
{
    private static final Logger log = Logger.get(ComponentRevision.class);

    public ComponentRevision
    {
        requireNonNull(kind, "kind is null");
        requireNonNull(name, "name is null");
    }

    public static ComponentRevision of(String kind, String name, Object component)
    {
        if (!(component instanceof LoadedConfiguration loadedConfiguration)) {
            return failed(kind, name, COMPONENT_DOES_NOT_REPORT);
        }
        try {
            return new ComponentRevision(kind, name, requireNonNull(loadedConfiguration.loadedRevision(), "loadedRevision is null"), null);
        }
        catch (UnsupportedOperationException e) {
            // The component can report, but was not given what it needs to; that is a configuration
            // answer rather than a failure, and it does not change until somebody changes the configuration
            log.debug(e, "%s '%s' is not configured to report what it has loaded", kind, name);
            return failed(kind, name, COMPONENT_NOT_CONFIGURED);
        }
        catch (RuntimeException e) {
            // An unreadable state is reported as such; it must never look like an acknowledged revision
            log.warn("%s '%s' could not report what it has loaded: %s", kind, name, summarize(e));
            log.debug(e, "%s '%s' could not report what it has loaded", kind, name);
            return failed(kind, name, COMPONENT_UNAVAILABLE);
        }
    }

    private static ComponentRevision failed(String kind, String name, CatalogSyncFailure failure)
    {
        return new ComponentRevision(kind, name, null, failure.name());
    }
}

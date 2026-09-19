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
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;

final class TestComponentRevision
{
    /**
     * What a driver, a file reader or an authorization backend puts in a message when it fails.
     * None of it may reach a caller of the readiness endpoint.
     */
    private static final String HOSTILE_MESSAGE =
            "FATAL: password authentication failed for user \"catalog_writer\" " +
                    "connecting to jdbc:postgresql://store.internal:5432/catalogs?user=catalog_writer&password=hunter2 " +
                    "while reading /etc/trino/secrets/password.db for org_17";

    @Test
    void testReportingComponent()
    {
        ComponentRevision revision = ComponentRevision.of("password-authenticator", "file", (LoadedConfiguration) () -> "sha256:abc");

        assertThat(revision.kind()).isEqualTo("password-authenticator");
        assertThat(revision.name()).isEqualTo("file");
        assertThat(revision.revision()).isEqualTo("sha256:abc");
        assertThat(revision.error()).isNull();
    }

    /**
     * Anything that cannot report what it loaded is listed without a revision. Silence must not
     * read as agreement, so there is no fallback value here.
     */
    @Test
    void testComponentThatCannotReport()
    {
        ComponentRevision revision = ComponentRevision.of("system-access-control", "opa", new Object());

        assertThat(revision.revision()).isNull();
        assertThat(revision.error()).isEqualTo("COMPONENT_DOES_NOT_REPORT");
    }

    @Test
    void testComponentThatIsNotConfiguredToReport()
    {
        ComponentRevision revision = ComponentRevision.of("system-access-control", "opa", (LoadedConfiguration) () -> {
            throw new UnsupportedOperationException("opa.policy.revision-uri is not configured, see " + HOSTILE_MESSAGE);
        });

        assertThat(revision.revision()).isNull();
        assertThat(revision.error()).isEqualTo("COMPONENT_NOT_CONFIGURED");
        assertThat(revision.toString()).doesNotContain("hunter2");
    }

    /**
     * The failure of a component is reported as a category. Its message belongs in the log, because
     * it routinely names the endpoint it could not reach, the file it could not read, and sometimes
     * the credentials it tried.
     */
    @Test
    void testFailureTextNeverReachesTheReport()
    {
        ComponentRevision revision = ComponentRevision.of("group-provider", "file", (LoadedConfiguration) () -> {
            throw new RuntimeException(HOSTILE_MESSAGE, new SQLException(HOSTILE_MESSAGE));
        });

        assertThat(revision.revision()).isNull();
        assertThat(revision.error()).isEqualTo("COMPONENT_UNAVAILABLE");
        assertThat(revision.toString())
                .doesNotContain("hunter2")
                .doesNotContain("store.internal")
                .doesNotContain("catalog_writer")
                .doesNotContain("password.db")
                .doesNotContain("org_17");
    }

    /**
     * The log line that replaces the message says which types failed, and nothing they carried.
     */
    @Test
    void testFailureSummaryIsOnlyTypes()
    {
        String summary = FailureSummary.summarize(new IllegalStateException(HOSTILE_MESSAGE, new SQLException(HOSTILE_MESSAGE)));

        assertThat(summary).isEqualTo("java.lang.IllegalStateException caused by java.sql.SQLException");
        assertThat(summary).doesNotContain("hunter2");
    }

    @Test
    void testFailureSummaryStopsAtACycle()
    {
        RuntimeException first = new RuntimeException(HOSTILE_MESSAGE);
        RuntimeException second = new RuntimeException(HOSTILE_MESSAGE, first);
        first.initCause(second);

        assertThat(FailureSummary.summarize(first))
                .isEqualTo("java.lang.RuntimeException caused by java.lang.RuntimeException");
    }
}

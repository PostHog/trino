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

import static org.assertj.core.api.Assertions.assertThat;

final class TestComponentRevision
{
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
        assertThat(revision.error()).contains("does not report the configuration it has loaded");
    }

    @Test
    void testComponentThatFailsToLoad()
    {
        ComponentRevision revision = ComponentRevision.of("group-provider", "file", (LoadedConfiguration) () -> {
            throw new IllegalStateException("group file is missing");
        });

        assertThat(revision.revision()).isNull();
        assertThat(revision.error()).isEqualTo("group file is missing");
    }
}

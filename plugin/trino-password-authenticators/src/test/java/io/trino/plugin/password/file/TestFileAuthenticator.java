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
package io.trino.plugin.password.file;

import io.trino.spi.security.AccessDeniedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFileAuthenticator
{
    @TempDir
    Path temporaryDirectory;

    @Test
    void testReservedUserCannotFallBackToPersistentPassword()
            throws IOException
    {
        String hash = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";
        String serviceUser = "example.svc_0123456789abcdef01234567";
        Path passwordFile = Files.writeString(temporaryDirectory.resolve("password.db"), serviceUser + ":" + hash + "\nexample.root:" + hash + "\nexample.svc_reporter:" + hash + "\n");
        FileConfig config = new FileConfig().setPasswordFile(passwordFile.toFile());
        assertThat(new FileAuthenticator(config).createAuthenticatedPrincipal(serviceUser, "user123").getName()).isEqualTo(serviceUser);

        var authenticator = new FileAuthenticator(config.setReservedUserRegex("(?:[^.]+[.])?svc_[0-9a-f]{24}"));
        assertThatThrownBy(() -> authenticator.createAuthenticatedPrincipal(serviceUser, "user123")).isInstanceOf(AccessDeniedException.class);
        assertThat(authenticator.createAuthenticatedPrincipal("example.root", "user123").getName()).isEqualTo("example.root");
        assertThat(authenticator.createAuthenticatedPrincipal("example.svc_reporter", "user123").getName()).isEqualTo("example.svc_reporter");
    }
}

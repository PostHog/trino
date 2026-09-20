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

import com.google.common.hash.Hashing;
import io.airlift.units.Duration;
import io.trino.spi.security.AccessDeniedException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A controller that publishes credentials or groups to a coordinator needs to know when that exact
 * process answers with them. These components report the data they have loaded, which is not the
 * same as the data that is on disk.
 */
final class TestLoadedConfigurationRevisions
{
    // user_1 with password "user123", hashed so that the file is a realistic password file
    private static final String USER_LINE = "user_1:$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";
    private static final String OTHER_USER_LINE = "user_2:$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";

    @TempDir
    private Path temporaryDirectory;

    @Test
    void testPasswordFileRevisionIsTheFingerprintOfTheLoadedFile()
            throws IOException
    {
        File passwordFile = write("password.db", USER_LINE + "\n");

        FileAuthenticator authenticator = new FileAuthenticator(new FileConfig()
                .setPasswordFile(passwordFile)
                .setRefreshPeriod(new Duration(1, MILLISECONDS)));

        assertThat(authenticator.loadedRevision()).isEqualTo(fingerprint(USER_LINE + "\n"));
        assertThat(authenticator.createAuthenticatedPrincipal("user_1", "user123").getName()).isEqualTo("user_1");
    }

    @Test
    void testPasswordFileRevisionFollowsAReload()
            throws Exception
    {
        File passwordFile = write("password.db", USER_LINE + "\n");
        FileAuthenticator authenticator = new FileAuthenticator(new FileConfig()
                .setPasswordFile(passwordFile)
                .setRefreshPeriod(new Duration(1, MILLISECONDS)));
        String before = authenticator.loadedRevision();

        write("password.db", USER_LINE + "\n" + OTHER_USER_LINE + "\n");
        Thread.sleep(50);

        assertThat(authenticator.loadedRevision()).isNotEqualTo(before);
        assertThat(authenticator.loadedRevision()).isEqualTo(fingerprint(USER_LINE + "\n" + OTHER_USER_LINE + "\n"));
        assertThat(authenticator.createAuthenticatedPrincipal("user_2", "user123").getName()).isEqualTo("user_2");
    }

    /**
     * The important half: a file that has been replaced but is not in effect yet must not be
     * reported as loaded, or a controller would open a gate this process cannot honor.
     */
    @Test
    void testRevisionDoesNotFollowAFileThatIsNotInEffectYet()
            throws IOException
    {
        File passwordFile = write("password.db", USER_LINE + "\n");
        FileAuthenticator authenticator = new FileAuthenticator(new FileConfig()
                .setPasswordFile(passwordFile)
                .setRefreshPeriod(new Duration(30, SECONDS)));
        String before = authenticator.loadedRevision();

        write("password.db", USER_LINE + "\n" + OTHER_USER_LINE + "\n");

        assertThat(authenticator.loadedRevision()).isEqualTo(before);
        assertThatThrownBy(() -> authenticator.createAuthenticatedPrincipal("user_2", "user123"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testGroupFileRevisionIsTheFingerprintOfTheLoadedFile()
            throws Exception
    {
        File groupFile = write("group.txt", "group_a:user_1\n");
        FileGroupProvider groupProvider = new FileGroupProvider(new FileGroupConfig()
                .setGroupFile(groupFile)
                .setRefreshPeriod(new Duration(1, MILLISECONDS)));

        assertThat(groupProvider.loadedRevision()).isEqualTo(fingerprint("group_a:user_1\n"));

        write("group.txt", "group_a:user_1,user_2\n");
        Thread.sleep(50);

        assertThat(groupProvider.loadedRevision()).isEqualTo(fingerprint("group_a:user_1,user_2\n"));
        assertThat(groupProvider.getGroups("user_2")).containsExactly("group_a");
    }

    /**
     * The fingerprint is a published contract: a controller computes it from the bytes it wrote.
     */
    private static String fingerprint(String content)
    {
        return "sha256:" + base16().lowerCase().encode(Hashing.sha256().hashBytes(content.getBytes(UTF_8)).asBytes());
    }

    private File write(String name, String content)
            throws IOException
    {
        Path file = temporaryDirectory.resolve(name);
        Files.writeString(file, content);
        return file.toFile();
    }
}

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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.AuthenticatedPasswordPrincipal;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend.DUCKLAKE;
import static io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend.HOGLAKE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFileAuthenticator
{
    private static final String BCRYPT_PASSWORD = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";
    private static final String PBKDF2_PASSWORD = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";

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

    @Test
    void testAuthenticatedIdentityClassification()
            throws IOException
    {
        Path passwordFile = Files.writeString(temporaryDirectory.resolve("classified.db"),
                """
                # trufflehog-auth:1
                # trufflehog-target {"principal":"example-db.alice","backend":"hoglake"}
                # trufflehog-target {"principal":"example-db.root","backend":"hoglake"}
                # trufflehog-target {"principal":"legacy-db.root","backend":"ducklake"}
                # trufflehog-operator {"principal":"__trino_ops"}
                """ +
                        "example-db.alice:" + BCRYPT_PASSWORD + "\n" +
                        "example-db.root:" + BCRYPT_PASSWORD + "\n" +
                        "legacy-db.root:" + PBKDF2_PASSWORD + "\n" +
                        "__trino_ops:" + BCRYPT_PASSWORD + "\n" +
                        "unclassified:" + BCRYPT_PASSWORD + "\n");
        var authenticator = new FileAuthenticator(new FileConfig().setPasswordFile(passwordFile.toFile()).setAuthMetadataEnabled(true));

        Identity alice = authenticator.createAuthenticatedIdentity("example-db.alice", "user123").orElseThrow();
        assertThat(alice.getUser()).isEqualTo("example-db.alice");
        assertThat(alice.getGroups()).isEmpty();
        assertThat(alice.getPrincipal()).contains(AuthenticatedPasswordPrincipal.tenant("example-db.alice", HOGLAKE));
        assertThat(authenticator.createAuthenticatedIdentity("example-db.root", "user123").orElseThrow().getPrincipal())
                .contains(AuthenticatedPasswordPrincipal.tenant("example-db.root", HOGLAKE));
        assertThat(authenticator.createAuthenticatedIdentity("legacy-db.root", "password").orElseThrow().getPrincipal())
                .contains(AuthenticatedPasswordPrincipal.tenant("legacy-db.root", DUCKLAKE));
        assertThat(authenticator.createAuthenticatedIdentity("__trino_ops", "user123").orElseThrow().getPrincipal())
                .contains(AuthenticatedPasswordPrincipal.operator("__trino_ops"));
        assertThat(authenticator.createAuthenticatedIdentity("unclassified", "user123").orElseThrow().getPrincipal())
                .contains(new BasicPrincipal("unclassified"));
        assertThat(authenticator.createAuthenticatedPrincipal("example-db.alice", "user123"))
                .isEqualTo(AuthenticatedPasswordPrincipal.tenant("example-db.alice", HOGLAKE));
        assertThatThrownBy(() -> authenticator.createAuthenticatedIdentity("example-db.alice", "incorrect"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessage("Access Denied: Invalid credentials");
        assertThatThrownBy(() -> authenticator.createAuthenticatedIdentity("missing", "user123"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testLegacyIdentityRemainsUnclassified()
            throws IOException
    {
        Path passwordFile = Files.writeString(temporaryDirectory.resolve("legacy.db"), "example-db.root:" + PBKDF2_PASSWORD + "\n__trino_ops:" + BCRYPT_PASSWORD + "\n");
        for (boolean metadataEnabled : new boolean[] {false, true}) {
            var authenticator = new FileAuthenticator(new FileConfig().setPasswordFile(passwordFile.toFile()).setAuthMetadataEnabled(metadataEnabled));
            Identity root = authenticator.createAuthenticatedIdentity("example-db.root", "password").orElseThrow();
            assertThat(root.getUser()).isEqualTo("example-db.root");
            assertThat(root.getGroups()).isEmpty();
            assertThat(root.getPrincipal()).contains(new BasicPrincipal("example-db.root"));
            assertThat(authenticator.createAuthenticatedIdentity("__trino_ops", "user123").orElseThrow().getPrincipal())
                    .contains(new BasicPrincipal("__trino_ops"));
        }
    }

    @Test
    void testOneCapturedStoreDuringFileReplacement()
            throws IOException
    {
        String beforeContent = "# trufflehog-auth:1\n# trufflehog-target {\"principal\":\"example-db.alice\",\"backend\":\"hoglake\"}\nexample-db.alice:" + PBKDF2_PASSWORD + "\n";
        String afterContent = "# trufflehog-auth:1\n# trufflehog-target {\"principal\":\"example-db.alice\",\"backend\":\"ducklake\"}\nexample-db.alice:" + BCRYPT_PASSWORD + "\n";
        Path passwordFile = Files.writeString(temporaryDirectory.resolve("replacement.db"), beforeContent);
        PasswordStore before = new PasswordStore(passwordFile.toFile(), 1000, true);
        var current = new AtomicReference<>(before);
        var captures = new AtomicInteger();
        var authenticator = new FileAuthenticator(() -> {
            PasswordStore captured = current.get();
            if (captures.incrementAndGet() == 1) {
                try {
                    Files.writeString(passwordFile, afterContent);
                    current.set(new PasswordStore(passwordFile.toFile(), 1000, true));
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return captured;
        }, Optional.empty());

        Identity first = authenticator.createAuthenticatedIdentity("example-db.alice", "password").orElseThrow();
        assertThat(captures.get()).isEqualTo(1);
        assertThat(first.getPrincipal()).contains(AuthenticatedPasswordPrincipal.tenant("example-db.alice", HOGLAKE));
        assertThat(before.principal("example-db.alice")).isEqualTo(AuthenticatedPasswordPrincipal.tenant("example-db.alice", HOGLAKE));
        assertThat(current.get().principal("example-db.alice")).isEqualTo(AuthenticatedPasswordPrincipal.tenant("example-db.alice", DUCKLAKE));
        assertThat(before.revision()).isNotEqualTo(current.get().revision());
        assertThat(authenticator.loadedRevision()).isEqualTo(current.get().revision().orElseThrow());
        assertThat(captures.get()).isEqualTo(2);

        assertThatThrownBy(() -> authenticator.createAuthenticatedIdentity("example-db.alice", "password"))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(captures.get()).isEqualTo(3);
        Identity second = authenticator.createAuthenticatedIdentity("example-db.alice", "user123").orElseThrow();
        assertThat(second.getPrincipal()).contains(AuthenticatedPasswordPrincipal.tenant("example-db.alice", DUCKLAKE));
        assertThat(captures.get()).isEqualTo(4);
    }

    @Test
    void testDeprecatedPrincipalPathAuthenticatesOnce()
    {
        var captures = new AtomicInteger();
        var verifications = new AtomicInteger();
        PasswordStore store = new PasswordStore(ImmutableMap.of("user", password -> {
            verifications.incrementAndGet();
            return password.equals("synthetic-password");
        }), 0);
        var authenticator = new FileAuthenticator(() -> {
            captures.incrementAndGet();
            return store;
        }, Optional.empty());

        assertThat(authenticator.createAuthenticatedPrincipal("user", "synthetic-password")).isEqualTo(new BasicPrincipal("user"));
        assertThat(captures.get()).isEqualTo(1);
        assertThat(verifications.get()).isEqualTo(1);
    }

    @Test
    void testReservedUserRejectsBeforeStoreCapture()
    {
        var captures = new AtomicInteger();
        var authenticator = new FileAuthenticator(() -> {
            captures.incrementAndGet();
            throw new AssertionError("Reserved users must not load the store");
        }, Optional.of(Pattern.compile("reserved")));

        assertThatThrownBy(() -> authenticator.createAuthenticatedIdentity("reserved", "synthetic-password"))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> authenticator.createAuthenticatedPrincipal("reserved", "synthetic-password"))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(captures.get()).isZero();
    }
}

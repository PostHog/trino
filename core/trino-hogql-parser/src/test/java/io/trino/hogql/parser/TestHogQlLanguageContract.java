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
package io.trino.hogql.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

public class TestHogQlLanguageContract
{
    private static final String LANGUAGE_RESOURCE_ROOT = "/io/trino/hogql/parser/language/1.0.0/";

    @Test
    public void testLoadsCurrentLanguageContract()
    {
        HogQlLanguageContract contract = HogQlLanguageContract.current();

        assertThat(contract.schemaVersion()).isEqualTo(1);
        assertThat(contract.languageVersion()).isEqualTo(new HogQlLanguageVersion(1, 0, 0));
        assertThat(contract.antlrVersion()).isEqualTo("4.13.2");
        assertThat(contract.canonicalParser()).isEqualTo("cpp-antlr");
        assertThat(contract.entryPoints()).containsEntry("query", "select");
        assertThat(contract.grammarSha256()).isEqualTo("c255b1c828ea34b4eb5373dd1a153bbcf69f4f9eb58a900d353972ce07724242");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "1", "1.0", "01.0.0", "1.0.0-beta", "-1.0.0"})
    public void testRejectsInvalidLanguageVersions(String value)
    {
        assertThatThrownBy(() -> HogQlLanguageVersion.valueOf(value))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid HogQL language version");
    }

    @Test
    public void testRejectsUnsupportedLanguageVersion()
    {
        assertThatThrownBy(() -> new HogQlParser().parseStatement("SELECT 1", new HogQlLanguageVersion(1, 1, 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unsupported HogQL language version: 1.1.0");
    }

    @Test
    public void testGrammarFilesMatchLanguageDescriptor()
            throws IOException
    {
        HogQlLanguageContract contract = HogQlLanguageContract.current();
        MessageDigest aggregate = sha256Digest();

        for (HogQlLanguageContract.GrammarFile file : contract.files()) {
            byte[] content = readResource(LANGUAGE_RESOURCE_ROOT + "grammar/" + file.path());
            assertThat(sha256(content)).isEqualTo(file.sha256());
            aggregate.update(file.path().getBytes(StandardCharsets.UTF_8));
            aggregate.update((byte) 0);
            aggregate.update(content);
            aggregate.update((byte) 0);
        }

        assertThat(HexFormat.of().formatHex(aggregate.digest())).isEqualTo(contract.grammarSha256());
    }

    private static byte[] readResource(String path)
            throws IOException
    {
        try (InputStream input = TestHogQlLanguageContract.class.getResourceAsStream(path)) {
            if (input == null) {
                fail("missing test resource: " + path);
            }
            return input.readAllBytes();
        }
    }

    private static String sha256(byte[] content)
    {
        return HexFormat.of().formatHex(sha256Digest().digest(content));
    }

    private static MessageDigest sha256Digest()
    {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }
}

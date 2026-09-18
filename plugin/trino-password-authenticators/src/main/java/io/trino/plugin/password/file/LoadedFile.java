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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Files;
import java.util.List;

import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * The content of a password or group file, together with a fingerprint of the exact bytes the
 * lines were parsed from.
 *
 * <p>Content and fingerprint come from one read, so a component reporting the fingerprint through
 * {@link io.trino.spi.security.LoadedConfiguration} can never claim to have loaded a version of
 * the file it did not actually parse.
 */
record LoadedFile(String revision, List<String> lines)
{
    /**
     * Line terminators as {@code Files.readAllLines} understands them, so that a file behaves the
     * same whether it was written with Unix, Windows or classic Mac line endings.
     */
    private static final Splitter LINE_SPLITTER = Splitter.onPattern("\\R");

    LoadedFile
    {
        requireNonNull(revision, "revision is null");
        lines = ImmutableList.copyOf(requireNonNull(lines, "lines is null"));
    }

    /**
     * @throws IOException if the file cannot be read or is not valid UTF-8, exactly as reading its
     *         lines directly would fail
     */
    static LoadedFile read(File file)
            throws IOException
    {
        byte[] content = Files.readAllBytes(file.toPath());
        return new LoadedFile(fingerprint(content), LINE_SPLITTER.splitToList(decodeUtf8(content)));
    }

    /**
     * The same content as a file whose lines are already known, for tests and callers that do not
     * read from disk.
     */
    static LoadedFile ofLines(List<String> lines)
    {
        return new LoadedFile(fingerprint(String.join("\n", lines).getBytes(UTF_8)), lines);
    }

    /**
     * Published contract: the fingerprint of a file is {@code sha256:} and the lower-case
     * hexadecimal SHA-256 of its bytes, so a controller that wrote the file can compute the value
     * it expects to see acknowledged without asking Trino what it means.
     */
    private static String fingerprint(byte[] content)
    {
        return "sha256:" + base16().lowerCase().encode(Hashing.sha256().hashBytes(content).asBytes());
    }

    private static String decodeUtf8(byte[] content)
            throws CharacterCodingException
    {
        // Reports malformed input instead of replacing it, so an unreadable file fails as before
        CharsetDecoder decoder = UTF_8.newDecoder();
        return decoder.decode(ByteBuffer.wrap(content)).toString();
    }
}

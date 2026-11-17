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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import io.trino.cache.NonEvictableCache;
import io.trino.spi.TrinoException;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.hash.Hashing.hmacSha256;
import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.plugin.password.file.EncryptionUtil.doesBCryptPasswordMatch;
import static io.trino.plugin.password.file.EncryptionUtil.doesPBKDF2PasswordMatch;
import static io.trino.plugin.password.file.EncryptionUtil.getHashingAlgorithm;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_UNAVAILABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PasswordStore
{
    private static final Splitter LINE_SPLITTER = Splitter.on(":").limit(2);

    private final Optional<String> revision;
    private final Map<String, HashedPassword> credentials;
    private final HashFunction passwordHasher;
    private final NonEvictableCache<CacheKey, Boolean> cache;

    public PasswordStore(File file, int cacheMaxSize)
    {
        this(readPasswordFile(file), cacheMaxSize);
    }

    private PasswordStore(LoadedFile passwordFile, int cacheMaxSize)
    {
        this(Optional.of(passwordFile.revision()), loadPasswordFile(passwordFile.lines()), cacheMaxSize);
    }

    @VisibleForTesting
    public PasswordStore(List<String> lines, int cacheMaxSize)
    {
        this(loadPasswordFile(lines), cacheMaxSize);
    }

    @VisibleForTesting
    PasswordStore(Map<String, HashedPassword> credentials, int cacheMaxSize)
    {
        // A store without a file has no file revision.
        this(Optional.empty(), credentials, cacheMaxSize);
    }

    private PasswordStore(Optional<String> revision, Map<String, HashedPassword> credentials, int cacheMaxSize)
    {
        this.revision = revision;
        this.credentials = ImmutableMap.copyOf(credentials);
        passwordHasher = createPasswordHasher();
        cache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(cacheMaxSize));
    }

    /**
     * Fingerprint of the credentials this store answers with, see
     * {@link io.trino.spi.security.LoadedConfiguration}. Empty when the credentials did not come
     * from a file.
     */
    public Optional<String> revision()
    {
        return revision;
    }

    public boolean authenticate(String user, String password)
    {
        requireNonNull(user, "user is null");
        requireNonNull(password, "password is null");
        // Preserve exact UTF-16 code units, including unpaired surrogates.
        // Length prefixes separate the username from the password.
        HashCode identifier = passwordHasher.newHasher()
                .putInt(user.length())
                .putUnencodedChars(user)
                .putInt(password.length())
                .putUnencodedChars(password)
                .hash();
        return uncheckedCacheGet(cache, new CacheKey(user, identifier), () -> matches(user, password));
    }

    @VisibleForTesting
    Map<?, Boolean> cachedAuthentications()
    {
        return ImmutableMap.copyOf(cache.asMap());
    }

    private boolean matches(String user, String password)
    {
        HashedPassword hashed = credentials.get(user);
        return (hashed != null) && hashed.matches(password);
    }

    private static HashFunction createPasswordHasher()
    {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        try {
            return hmacSha256(key);
        }
        finally {
            Arrays.fill(key, (byte) 0);
        }
    }

    private static Map<String, HashedPassword> loadPasswordFile(List<String> lines)
    {
        Map<String, HashedPassword> users = new HashMap<>();
        for (int lineNumber = 1; lineNumber <= lines.size(); lineNumber++) {
            String line = lines.get(lineNumber - 1).trim();
            if (line.isEmpty()) {
                continue;
            }

            List<String> parts = LINE_SPLITTER.splitToList(line);
            if (parts.size() != 2) {
                throw invalidFile(lineNumber, "Expected two parts for user and password", null);
            }
            String user = parts.get(0);
            String password = parts.get(1);

            try {
                if (users.put(user, getHashedPassword(password)) != null) {
                    throw invalidFile(lineNumber, "Duplicate user: " + user, null);
                }
            }
            catch (HashedPasswordException e) {
                throw invalidFile(lineNumber, e.getMessage(), e);
            }
        }
        return ImmutableMap.copyOf(users);
    }

    private static RuntimeException invalidFile(int lineNumber, String message, Throwable cause)
    {
        return new TrinoException(CONFIGURATION_INVALID, format("Error in password file line %s: %s", lineNumber, message), cause);
    }

    private static LoadedFile readPasswordFile(File file)
    {
        try {
            return LoadedFile.read(file);
        }
        catch (IOException e) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Failed to read password file: " + file, e);
        }
    }

    private static HashedPassword getHashedPassword(String hashedPassword)
    {
        return switch (getHashingAlgorithm(hashedPassword)) {
            case BCRYPT -> password -> doesBCryptPasswordMatch(password, hashedPassword);
            case PBKDF2 -> password -> doesPBKDF2PasswordMatch(password, hashedPassword);
        };
    }

    private record CacheKey(String user, HashCode identifier)
    {
        @Override
        public String toString()
        {
            return "CacheKey{redacted}";
        }
    }

    public interface HashedPassword
    {
        boolean matches(String password);
    }
}

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import io.trino.cache.NonEvictableCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.password.file.EncryptionUtil.doesBCryptPasswordMatch;
import static io.trino.plugin.password.file.EncryptionUtil.doesPBKDF2PasswordMatch;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.lang.Thread.State.WAITING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPasswordStore
{
    private static final String BCRYPT_PASSWORD = "$2y$10$BqTb8hScP5DfcpmHo5PeyugxHz5Ky/qf3wrpD7SNm8sWuA3VlGqsa";
    private static final String PBKDF2_PASSWORD = "1000:5b4240333032306164:f38d165fce8ce42f59d366139ef5d9e1ca1247f0e06e503ee1a611dd9ec40876bb5edb8409f5abe5504aab6628e70cfb3d3a18e99d70357d295002c3d0a308a0";

    @Test
    public void testAuthenticate()
    {
        PasswordStore store = createStore("userbcrypt:" + BCRYPT_PASSWORD, "userpbkdf2:" + PBKDF2_PASSWORD);

        assertThat(store.authenticate("userbcrypt", "user123")).isTrue();
        assertThat(store.authenticate("userbcrypt", "user999")).isFalse();
        assertThat(store.authenticate("userbcrypt", "password")).isFalse();

        assertThat(store.authenticate("userpbkdf2", "password")).isTrue();
        assertThat(store.authenticate("userpbkdf2", "password999")).isFalse();
        assertThat(store.authenticate("userpbkdf2", "user123")).isFalse();

        assertThat(store.authenticate("baduser", "user123")).isFalse();
        assertThat(store.authenticate("baduser", "password")).isFalse();
    }

    @Test
    public void testCachesVerificationOutcomes()
    {
        AtomicInteger bcryptComparisons = new AtomicInteger();
        AtomicInteger pbkdf2Comparisons = new AtomicInteger();
        PasswordStore store = new PasswordStore(ImmutableMap.of(
                "userbcrypt", password -> {
                    bcryptComparisons.incrementAndGet();
                    return doesBCryptPasswordMatch(password, BCRYPT_PASSWORD);
                },
                "userpbkdf2", password -> {
                    pbkdf2Comparisons.incrementAndGet();
                    return doesPBKDF2PasswordMatch(password, PBKDF2_PASSWORD);
                }), 1000);

        for (int attempt = 0; attempt < 2; attempt++) {
            assertThat(store.authenticate("userbcrypt", "user123")).isTrue();
            assertThat(store.authenticate("userbcrypt", "password")).isFalse();
            assertThat(store.authenticate("userpbkdf2", "password")).isTrue();
            assertThat(store.authenticate("userpbkdf2", "user123")).isFalse();
            assertThat(store.authenticate("missing", "password")).isFalse();
        }

        assertThat(bcryptComparisons.get()).isEqualTo(2);
        assertThat(pbkdf2Comparisons.get()).isEqualTo(2);
        assertThat(store.cachedAuthentications()).hasSize(5);
        assertThat(store.cachedAuthentications().values()).containsExactlyInAnyOrder(true, false, true, false, false);
    }

    @Test
    public void testUserBinding()
            throws ReflectiveOperationException
    {
        AtomicInteger comparisons = new AtomicInteger();
        PasswordStore store = new PasswordStore(ImmutableMap.of(
                "first", password -> {
                    comparisons.incrementAndGet();
                    return doesPBKDF2PasswordMatch(password, PBKDF2_PASSWORD);
                },
                "second", password -> {
                    comparisons.incrementAndGet();
                    return doesBCryptPasswordMatch(password, BCRYPT_PASSWORD);
                }), 1000);

        for (int attempt = 0; attempt < 2; attempt++) {
            assertThat(store.authenticate("first", "password")).isTrue();
            assertThat(store.authenticate("second", "password")).isFalse();
        }

        assertThat(comparisons.get()).isEqualTo(2);
        List<?> keys = ImmutableList.copyOf(store.cachedAuthentications().keySet());
        assertThat(identifier(keys.get(0)).equals(identifier(keys.get(1)))).isFalse();
    }

    @Test
    public void testExactCodeUnits()
            throws ReflectiveOperationException
    {
        AtomicInteger comparisons = new AtomicInteger();
        PasswordStore store = new PasswordStore(ImmutableMap.of("user", password -> {
            comparisons.incrementAndGet();
            return password.equals("\uD800");
        }), 1000);
        List<String> passwords = ImmutableList.of("\uD800", "\uD801", "\uDC00", "?", "\uFFFD", "\uD83D\uDE00", "\u00E9", "e\u0301", "", "x\u0000y");

        for (int attempt = 0; attempt < 2; attempt++) {
            for (String password : passwords) {
                assertThat(store.authenticate("user", password)).isEqualTo(password.equals("\uD800"));
            }
        }

        assertThat(comparisons.get()).isEqualTo(passwords.size());
        assertThat(store.cachedAuthentications()).hasSize(passwords.size());
        for (Object key : store.cachedAuthentications().keySet()) {
            assertThat(identifier(key).bits()).isEqualTo(256);
        }
    }

    @Test
    public void testExactUsernamesAndFraming()
            throws ReflectiveOperationException
    {
        PasswordStore store = new PasswordStore(ImmutableMap.of(
                "ab", password -> password.equals("c"),
                "a", password -> password.equals("bc"),
                "\uD800", password -> password.isEmpty(),
                "\uD801", _ -> false,
                "\u00E9", password -> password.isEmpty(),
                "e\u0301", _ -> false), 1000);

        assertThat(store.authenticate("ab", "c")).isTrue();
        assertThat(store.authenticate("a", "bc")).isTrue();
        assertThat(store.authenticate("\uD800", "")).isTrue();
        assertThat(store.authenticate("\uD801", "")).isFalse();
        assertThat(store.authenticate("\u00E9", "")).isTrue();
        assertThat(store.authenticate("e\u0301", "")).isFalse();
        assertThat(store.cachedAuthentications()).hasSize(6);

        List<?> keys = ImmutableList.copyOf(store.cachedAuthentications().keySet());
        for (int first = 0; first < keys.size(); first++) {
            for (int second = first + 1; second < keys.size(); second++) {
                assertThat(identifier(keys.get(first)).equals(identifier(keys.get(second)))).isFalse();
            }
        }
    }

    @Test
    public void testNullCredentials()
    {
        PasswordStore store = createStore();
        assertThatThrownBy(() -> store.authenticate(null, "password"))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("user is null");
        assertThatThrownBy(() -> store.authenticate("user", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("password is null");
        assertThat(store.cachedAuthentications()).isEmpty();
    }

    @Test
    public void testZeroSize()
    {
        AtomicInteger comparisons = new AtomicInteger();
        PasswordStore store = new PasswordStore(ImmutableMap.of("user", password -> {
            comparisons.incrementAndGet();
            return doesPBKDF2PasswordMatch(password, PBKDF2_PASSWORD);
        }), 0);

        for (int attempt = 0; attempt < 2; attempt++) {
            assertThat(store.authenticate("user", "password")).isTrue();
            assertThat(store.authenticate("user", "wrong")).isFalse();
        }

        assertThat(comparisons.get()).isEqualTo(4);
        assertThat(store.cachedAuthentications()).isEmpty();
    }

    @Test
    public void testEviction()
    {
        AtomicInteger comparisons = new AtomicInteger();
        PasswordStore store = new PasswordStore(ImmutableMap.of("user", password -> {
            comparisons.incrementAndGet();
            return doesPBKDF2PasswordMatch(password, PBKDF2_PASSWORD);
        }), 1);

        assertThat(store.authenticate("user", "password")).isTrue();
        assertThat(store.authenticate("user", "wrong")).isFalse();
        assertThat(store.authenticate("user", "different")).isFalse();
        assertThat(comparisons.get()).isEqualTo(3);
        assertThat(store.cachedAuthentications()).hasSize(1);

        assertThat(store.authenticate("user", "password")).isTrue();
        assertThat(store.authenticate("user", "wrong")).isFalse();
        assertThat(store.authenticate("user", "different")).isFalse();
        assertThat(comparisons.get()).isBetween(5, 6);
        assertThat(store.cachedAuthentications()).hasSize(1);
    }

    @Test
    @Timeout(10)
    public void testConcurrentSuccessfulLoads()
            throws Exception
    {
        assertConcurrentLoads(0, "password", true);
        assertConcurrentLoads(1000, "password", true);
    }

    @Test
    @Timeout(10)
    public void testConcurrentDeniedLoads()
            throws Exception
    {
        assertConcurrentLoads(0, "wrong", false);
        assertConcurrentLoads(1000, "wrong", false);
    }

    @Test
    public void testLoadFailuresAreNotCached()
    {
        AtomicInteger comparisons = new AtomicInteger();
        RuntimeException failure = new IllegalStateException("Verification unavailable");
        PasswordStore store = new PasswordStore(ImmutableMap.of("user", password -> {
            if (comparisons.getAndIncrement() == 0) {
                throw failure;
            }
            return doesPBKDF2PasswordMatch(password, PBKDF2_PASSWORD);
        }), 1000);

        assertThatThrownBy(() -> store.authenticate("user", "password"))
                .isInstanceOf(UncheckedExecutionException.class)
                .hasCause(failure);
        assertThat(store.cachedAuthentications()).isEmpty();
        assertThat(store.authenticate("user", "password")).isTrue();
        assertThat(store.authenticate("user", "password")).isTrue();
        assertThat(comparisons.get()).isEqualTo(2);

        AssertionError error = new AssertionError("Verification failed");
        PasswordStore broken = new PasswordStore(ImmutableMap.of("user", _ -> {
            throw error;
        }), 1000);
        assertThatThrownBy(() -> broken.authenticate("user", "password"))
                .isInstanceOf(ExecutionError.class)
                .hasCause(error);
        assertThat(broken.cachedAuthentications()).isEmpty();
    }

    @Test
    public void testCompletedCacheContainsOnlyIdentifiersAndOutcomes()
            throws ReflectiveOperationException
    {
        PasswordStore store = createStore("userbcrypt:" + BCRYPT_PASSWORD);
        assertThat(store.authenticate("userbcrypt", "user123")).isTrue();
        assertThat(store.authenticate("userbcrypt", "user999")).isFalse();
        Map<?, Boolean> entries = store.cachedAuthentications();
        assertThat(entries).hasSize(2);
        assertThat(entries.values()).containsExactlyInAnyOrder(true, false);
        assertThat(PasswordStore.class.getDeclaredField("cache").getType()).isEqualTo(NonEvictableCache.class);

        for (Object key : entries.keySet()) {
            assertThat(key.getClass().getDeclaredFields())
                    .extracting(Field::getName)
                    .containsExactlyInAnyOrder("user", "identifier");
            assertThat(key.getClass().getDeclaredFields())
                    .extracting(Field::getType)
                    .containsExactlyInAnyOrder(String.class, HashCode.class);
            Field user = key.getClass().getDeclaredField("user");
            user.setAccessible(true);
            assertThat(user.get(key)).isEqualTo("userbcrypt");
            assertThat(identifier(key).bits()).isEqualTo(256);
            assertThat(key.toString()).isEqualTo("CacheKey{redacted}");
        }
        assertThat(entries.toString()).doesNotContain("user123", "user999", "userbcrypt");
    }

    @Test
    public void testStoresHaveIndependentIdentifiers()
            throws ReflectiveOperationException
    {
        PasswordStore first = createStore("userbcrypt:" + BCRYPT_PASSWORD);
        PasswordStore second = createStore("userbcrypt:" + BCRYPT_PASSWORD);
        assertThat(first.authenticate("userbcrypt", "user123")).isTrue();
        assertThat(second.authenticate("userbcrypt", "user123")).isTrue();
        Object firstKey = first.cachedAuthentications().keySet().iterator().next();
        Object secondKey = second.cachedAuthentications().keySet().iterator().next();
        assertThat(identifier(firstKey).equals(identifier(secondKey))).isFalse();
    }

    @Test
    public void testEmptyFile()
    {
        createStore();
    }

    @Test
    public void testInvalidFile()
    {
        assertThatThrownBy(() -> createStore("", "junk"))
                .hasMessage("Error in password file line 2: Expected two parts for user and password");

        assertThatThrownBy(() -> createStore("abc:" + BCRYPT_PASSWORD, "xyz:" + BCRYPT_PASSWORD, "abc:" + PBKDF2_PASSWORD))
                .hasMessage("Error in password file line 3: Duplicate user: abc");

        assertThatThrownBy(() -> createStore("x:x"))
                .hasMessage("Error in password file line 1: Password hashing algorithm cannot be determined");

        assertThatThrownBy(() -> createStore("x:$2y$xxx"))
                .hasMessage("Error in password file line 1: Invalid BCrypt password");

        assertThatThrownBy(() -> createStore("x:x:x"))
                .hasMessage("Error in password file line 1: Invalid PBKDF2 password");
    }

    private static PasswordStore createStore(String... lines)
    {
        return new PasswordStore(ImmutableList.copyOf(lines), 1000);
    }

    private static HashCode identifier(Object key)
            throws ReflectiveOperationException
    {
        Field identifier = key.getClass().getDeclaredField("identifier");
        identifier.setAccessible(true);
        return (HashCode) identifier.get(key);
    }

    private static void assertConcurrentLoads(int cacheMaxSize, String password, boolean expected)
            throws Exception
    {
        AtomicInteger comparisons = new AtomicInteger();
        AtomicInteger otherComparisons = new AtomicInteger();
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        PasswordStore store = new PasswordStore(ImmutableMap.of(
                "user", supplied -> {
                    comparisons.incrementAndGet();
                    if (supplied.equals(password)) {
                        loadStarted.countDown();
                        await(releaseLoad);
                    }
                    return doesPBKDF2PasswordMatch(supplied, PBKDF2_PASSWORD);
                },
                "other", supplied -> {
                    otherComparisons.incrementAndGet();
                    return doesPBKDF2PasswordMatch(supplied, PBKDF2_PASSWORD);
                }), cacheMaxSize);
        FutureTask<Boolean> first = new FutureTask<>(() -> store.authenticate("user", password));
        FutureTask<Boolean> second = new FutureTask<>(() -> store.authenticate("user", password));
        Thread firstThread = Thread.ofPlatform().unstarted(first);
        Thread secondThread = Thread.ofPlatform().unstarted(second);
        try {
            firstThread.start();
            assertThat(loadStarted.await(5, SECONDS)).isTrue();
            secondThread.start();
            assertEventually(new Duration(5, SECONDS), () -> assertThat(secondThread.getState()).isEqualTo(WAITING));
            assertThat(comparisons.get()).isEqualTo(1);

            assertThat(store.authenticate("user", "different")).isFalse();
            assertThat(store.authenticate("other", "password")).isTrue();
            assertThat(comparisons.get()).isEqualTo(2);
            assertThat(otherComparisons.get()).isEqualTo(1);

            releaseLoad.countDown();
            assertThat(first.get(5, SECONDS)).isEqualTo(expected);
            assertThat(second.get(5, SECONDS)).isEqualTo(expected);
            assertThat(comparisons.get()).isEqualTo(2);
            assertThat(store.authenticate("user", password)).isEqualTo(expected);
            if (cacheMaxSize == 0) {
                assertThat(comparisons.get()).isEqualTo(3);
                assertThat(store.cachedAuthentications()).isEmpty();
            }
            else {
                assertThat(comparisons.get()).isEqualTo(2);
                assertThat(store.cachedAuthentications()).hasSize(3);
            }
        }
        finally {
            releaseLoad.countDown();
            first.cancel(true);
            second.cancel(true);
            firstThread.join(5000);
            secondThread.join(5000);
            assertThat(firstThread.isAlive()).isFalse();
            assertThat(secondThread.isAlive()).isFalse();
        }
    }

    private static void await(CountDownLatch latch)
    {
        try {
            assertThat(latch.await(5, SECONDS)).isTrue();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}

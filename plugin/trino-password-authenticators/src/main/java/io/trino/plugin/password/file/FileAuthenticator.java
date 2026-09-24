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

import com.google.inject.Inject;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.LoadedConfiguration;
import io.trino.spi.security.PasswordAuthenticator;

import java.io.File;
import java.security.Principal;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FileAuthenticator
        implements PasswordAuthenticator, LoadedConfiguration
{
    private final Supplier<PasswordStore> passwordStoreSupplier;
    private final Optional<Pattern> reservedUserPattern;

    @Inject
    public FileAuthenticator(FileConfig config)
    {
        reservedUserPattern = Optional.ofNullable(config.getReservedUserRegex()).map(Pattern::compile);
        File file = config.getPasswordFile();
        int cacheMaxSize = config.getAuthTokenCacheMaxSize();

        passwordStoreSupplier = memoizeWithExpiration(
                () -> new PasswordStore(file, cacheMaxSize),
                config.getRefreshPeriod().toMillis(),
                MILLISECONDS);
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        if (reservedUserPattern.filter(pattern -> pattern.matcher(user).matches()).isPresent() || !passwordStoreSupplier.get().authenticate(user, password)) {
            throw new AccessDeniedException("Invalid credentials");
        }

        return new BasicPrincipal(user);
    }

    /**
     * Fingerprint of the password file this authenticator would authenticate against right now.
     * It comes from the loaded store, so a file that was replaced but is not in effect yet - the
     * refresh period has not elapsed - is not reported as loaded.
     */
    @Override
    public String loadedRevision()
    {
        return passwordStoreSupplier.get().revision()
                .orElseThrow(() -> new IllegalStateException("Password store was not loaded from a file"));
    }
}

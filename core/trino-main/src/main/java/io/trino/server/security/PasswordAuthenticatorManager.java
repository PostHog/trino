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
package io.trino.server.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.log.Logger;
import io.trino.server.ComponentRevision;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.util.Objects.requireNonNull;

public class PasswordAuthenticatorManager
{
    private static final Logger log = Logger.get(PasswordAuthenticatorManager.class);

    private static final String NAME_PROPERTY = "password-authenticator.name";

    private final List<File> configFiles;
    private final AtomicBoolean required = new AtomicBoolean();
    private final Map<String, PasswordAuthenticatorFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<List<PasswordAuthenticator>> authenticators = new AtomicReference<>();
    /**
     * Names of the loaded authenticators, in the order of {@link #authenticators}, so that a
     * readiness report can say which implementation a revision belongs to.
     */
    private final AtomicReference<List<String>> authenticatorNames = new AtomicReference<>(ImmutableList.of());
    private final SecretsResolver secretsResolver;

    @Inject
    public PasswordAuthenticatorManager(PasswordAuthenticatorConfig config, SecretsResolver secretsResolver)
    {
        this.configFiles = ImmutableList.copyOf(config.getPasswordAuthenticatorFiles());
        checkArgument(!configFiles.isEmpty(), "password authenticator files list is empty");
        this.secretsResolver = requireNonNull(secretsResolver, "secretsResolver is null");
    }

    public void setRequired()
    {
        required.set(true);
    }

    public void addPasswordAuthenticatorFactory(PasswordAuthenticatorFactory factory)
    {
        checkArgument(factories.putIfAbsent(factory.getName(), factory) == null,
                "Password authenticator '%s' is already registered",
                factory.getName());
    }

    public boolean isLoaded()
    {
        return authenticators.get() != null;
    }

    public void loadPasswordAuthenticator()
    {
        if (!required.get()) {
            return;
        }

        ImmutableList.Builder<PasswordAuthenticator> authenticators = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();
        for (File configFile : configFiles) {
            NamedAuthenticator authenticator = loadAuthenticator(configFile.getAbsoluteFile());
            authenticators.add(authenticator.authenticator());
            names.add(authenticator.name());
        }
        this.authenticatorNames.set(names.build());
        this.authenticators.set(authenticators.build());
    }

    private NamedAuthenticator loadAuthenticator(File configFile)
    {
        Map<String, String> properties;
        try {
            properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Password authenticator configuration %s does not contain '%s'", configFile, NAME_PROPERTY);

        log.info("-- Loading password authenticator --");

        PasswordAuthenticatorFactory factory = factories.get(name);
        checkState(factory != null, "Password authenticator '%s' is not registered", name);

        PasswordAuthenticator authenticator;
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            authenticator = factory.create(ImmutableMap.copyOf(secretsResolver.getResolvedConfiguration(properties)));
        }

        log.info("-- Loaded password authenticator %s --", name);
        return new NamedAuthenticator(name, authenticator);
    }

    public List<PasswordAuthenticator> getAuthenticators()
    {
        checkState(isLoaded(), "authenticators were not loaded");
        return authenticators.get();
    }

    /**
     * What each loaded authenticator reports about the credentials it currently authenticates
     * against. Empty while no authenticator is loaded, which is not an acknowledgement either.
     */
    public List<ComponentRevision> loadedRevisions()
    {
        List<PasswordAuthenticator> loaded = authenticators.get();
        if (loaded == null) {
            return ImmutableList.of();
        }
        List<String> names = authenticatorNames.get();
        ImmutableList.Builder<ComponentRevision> revisions = ImmutableList.builder();
        for (int i = 0; i < loaded.size(); i++) {
            String name = i < names.size() ? names.get(i) : loaded.get(i).getClass().getSimpleName();
            revisions.add(ComponentRevision.of("password-authenticator", name, loaded.get(i)));
        }
        return revisions.build();
    }

    @VisibleForTesting
    public void setAuthenticators(PasswordAuthenticator... authenticators)
    {
        if (!this.authenticators.compareAndSet(null, ImmutableList.copyOf(authenticators))) {
            throw new IllegalStateException("authenticators already loaded");
        }
    }

    private record NamedAuthenticator(String name, PasswordAuthenticator authenticator)
    {
        private NamedAuthenticator
        {
            requireNonNull(name, "name is null");
            requireNonNull(authenticator, "authenticator is null");
        }
    }
}

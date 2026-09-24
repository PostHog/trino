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
package io.trino.plugin.password.duckgres;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DuckgresServiceCredentialAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "duckgres-service-credential";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        Injector injector = new Bootstrap(
                "io.trino.bootstrap.auth." + getName(),
                binder -> {
                    configBinder(binder).bindConfig(DuckgresServiceCredentialConfig.class);
                    binder.bind(DuckgresServiceCredentialAuthenticator.class).in(Scopes.SINGLETON);
                    httpClientBinder(binder)
                            .bindHttpClient("duckgres-service-credential", ForDuckgresServiceCredential.class)
                            .withConfigDefaults(client -> client
                                    .setConnectTimeout(new Duration(2, SECONDS))
                                    .setRequestTimeout(new Duration(5, SECONDS))
                                    .setMaxResponseContentLength(DataSize.of(64, KILOBYTE)));
                })
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(DuckgresServiceCredentialAuthenticator.class);
    }
}

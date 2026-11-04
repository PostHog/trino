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
package io.trino.plugin.posthogexternal;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Wraps the Delta connector so CREATE TABLE and CTAS are refused before Delta writes a transaction log.
 * <p>
 * The access control cannot refuse them: {@code register_table} runs the same create table check.
 * The procedure writes to the metastore directly, so refusing the metadata calls leaves it working.
 */
final class CreateTableRefusingConnector
{
    private static final Set<String> REFUSED_METADATA_METHODS = ImmutableSet.of("createTable", "beginCreateTable");

    private CreateTableRefusingConnector() {}

    static Connector wrap(Connector connector)
    {
        return proxy(Connector.class, connector, (method, args) -> {
            Object result = invoke(connector, method, args);
            if (method.getName().equals("getMetadata")) {
                ConnectorMetadata metadata = (ConnectorMetadata) result;
                return proxy(ConnectorMetadata.class, metadata, (metadataMethod, metadataArgs) -> {
                    if (REFUSED_METADATA_METHODS.contains(metadataMethod.getName())) {
                        throw new TrinoException(NOT_SUPPORTED, "Tables in this catalog are read-only. Add an existing Delta table with system.register_table");
                    }
                    return invoke(metadata, metadataMethod, metadataArgs);
                });
            }
            return result;
        });
    }

    private interface Call
    {
        Object call(Method method, Object[] args)
                throws Throwable;
    }

    private static <T> T proxy(Class<T> type, T delegate, Call call)
    {
        requireNonNull(delegate, "delegate is null");
        InvocationHandler handler = (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                    case "equals" -> proxy == args[0];
                    case "hashCode" -> System.identityHashCode(proxy);
                    default -> method.invoke(delegate, args);
                };
            }
            return call.call(method, args);
        };
        return type.cast(Proxy.newProxyInstance(CreateTableRefusingConnector.class.getClassLoader(), new Class<?>[] {type}, handler));
    }

    private static Object invoke(Object target, Method method, Object[] args)
            throws Throwable
    {
        try {
            return method.invoke(target, args);
        }
        catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}

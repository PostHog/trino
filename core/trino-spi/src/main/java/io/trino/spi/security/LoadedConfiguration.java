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
package io.trino.spi.security;

/**
 * An optional capability of a security component - a {@link PasswordAuthenticator}, a
 * {@link GroupProvider} or a {@link SystemAccessControl} - whose decisions are based on data that
 * is published to it, such as a file that an external controller writes and the component reloads.
 *
 * <p>A controller that publishes such data needs to know when a specific process actually uses it.
 * A file having been written, a config map having been updated or a refresh interval having
 * elapsed are not proof of that. Implementing this interface lets a component report which data it
 * would answer with right now.
 *
 * <p>The returned value is opaque and only has to be comparable with what the publisher expects
 * for the same data. It must never expose credentials or other secrets, so it is a fingerprint and
 * not the data itself. The component must report the data it currently has loaded; reporting data
 * that has been written but not yet loaded would let a controller act on an acknowledgement the
 * component cannot honor.
 */
public interface LoadedConfiguration
{
    /**
     * Fingerprint of the configuration data that is in effect in this process right now.
     *
     * @throws RuntimeException if the data cannot be loaded; an unreadable state must not be
     *         reported as a fingerprint
     */
    String loadedRevision();
}

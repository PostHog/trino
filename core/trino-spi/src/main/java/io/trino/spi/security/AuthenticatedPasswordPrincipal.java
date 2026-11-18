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

import java.security.Principal;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class AuthenticatedPasswordPrincipal
        implements Principal
{
    public enum Backend
    {
        HOGLAKE,
        DUCKLAKE,
    }

    private final String name;
    private final Optional<Backend> backend;
    private final boolean operator;

    private AuthenticatedPasswordPrincipal(String name, Optional<Backend> backend, boolean operator)
    {
        this.name = requireNonNull(name, "name is null");
        this.backend = requireNonNull(backend, "backend is null");
        this.operator = operator;
    }

    public static AuthenticatedPasswordPrincipal tenant(String verifiedPrincipal, Backend backend)
    {
        return new AuthenticatedPasswordPrincipal(verifiedPrincipal, Optional.of(backend), false);
    }

    public static AuthenticatedPasswordPrincipal operator(String verifiedPrincipal)
    {
        return new AuthenticatedPasswordPrincipal(verifiedPrincipal, Optional.empty(), true);
    }

    @Override
    public String getName()
    {
        return name;
    }

    public Optional<Backend> backend()
    {
        return backend;
    }

    public boolean isOperator()
    {
        return operator;
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public boolean equals(Object object)
    {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        AuthenticatedPasswordPrincipal that = (AuthenticatedPasswordPrincipal) object;
        return operator == that.operator && name.equals(that.name) && backend.equals(that.backend);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, backend, operator);
    }
}

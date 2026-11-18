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

import org.junit.jupiter.api.Test;

import static io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend.DUCKLAKE;
import static io.trino.spi.security.AuthenticatedPasswordPrincipal.Backend.HOGLAKE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class TestAuthenticatedPasswordPrincipal
{
    @Test
    void testTenant()
    {
        var principal = AuthenticatedPasswordPrincipal.tenant("example-db.alice", HOGLAKE);
        assertThat(principal.getName()).isEqualTo("example-db.alice");
        assertThat(principal.toString()).isEqualTo("example-db.alice");
        assertThat(principal.backend()).contains(HOGLAKE);
        assertThat(principal.isOperator()).isFalse();
        assertThat(principal).isEqualTo(AuthenticatedPasswordPrincipal.tenant("example-db.alice", HOGLAKE));
        assertThat(principal.hashCode()).isEqualTo(AuthenticatedPasswordPrincipal.tenant("example-db.alice", HOGLAKE).hashCode());
        assertThat(principal).isNotEqualTo(AuthenticatedPasswordPrincipal.tenant("example-db.alice", DUCKLAKE));
        assertThat(principal).isNotEqualTo(AuthenticatedPasswordPrincipal.tenant("example-db.root", HOGLAKE));
        assertThat(principal).isNotEqualTo(AuthenticatedPasswordPrincipal.operator("example-db.alice"));
        assertThat(principal).isNotEqualTo(new BasicPrincipal("example-db.alice"));
        assertThat(principal).isNotEqualTo(null);
    }

    @Test
    void testOperator()
    {
        var principal = AuthenticatedPasswordPrincipal.operator("__trino_ops");
        assertThat(principal.getName()).isEqualTo("__trino_ops");
        assertThat(principal.toString()).isEqualTo("__trino_ops");
        assertThat(principal.backend()).isEmpty();
        assertThat(principal.isOperator()).isTrue();
        assertThat(principal).isEqualTo(AuthenticatedPasswordPrincipal.operator("__trino_ops"));
        assertThat(principal.hashCode()).isEqualTo(AuthenticatedPasswordPrincipal.operator("__trino_ops").hashCode());
    }

    @Test
    void testNulls()
    {
        assertThatNullPointerException().isThrownBy(() -> AuthenticatedPasswordPrincipal.tenant(null, HOGLAKE));
        assertThatNullPointerException().isThrownBy(() -> AuthenticatedPasswordPrincipal.tenant("example-db.alice", null));
        assertThatNullPointerException().isThrownBy(() -> AuthenticatedPasswordPrincipal.operator(null));
    }
}

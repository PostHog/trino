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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHostQualifiedUsers
{
    private static final HostQualifiedUsers USERS = new HostQualifiedUsers(
            ImmutableList.of("tenants.example.com"),
            ImmutableSet.of("coordinator"));

    @Test
    void testQualifiesUserWithTenantLabel()
    {
        assertThat(USERS.qualify("alice", Optional.of("tenant-a.tenants.example.com"))).isEqualTo("tenant-a.alice");
        assertThat(USERS.qualify("alice", Optional.of("Tenant-A.Tenants.Example.COM"))).isEqualTo("tenant-a.alice");
        assertThat(USERS.qualify("alice", Optional.of("tenant-a.tenants.example.com."))).isEqualTo("tenant-a.alice");
    }

    @Test
    void testLeavesUserUnchangedOutsideTenantHosts()
    {
        // the domain itself names no tenant
        assertThat(USERS.qualify("alice", Optional.of("tenants.example.com"))).isEqualTo("alice");
        // a host outside the domain, including one that only ends with the same characters
        assertThat(USERS.qualify("alice", Optional.of("tenant-a.other.example.com"))).isEqualTo("alice");
        assertThat(USERS.qualify("alice", Optional.of("tenant-atenants.example.com"))).isEqualTo("alice");
        assertThat(USERS.qualify("alice", Optional.of("localhost"))).isEqualTo("alice");
        assertThat(USERS.qualify("alice", Optional.of("127.0.0.1"))).isEqualTo("alice");
        assertThat(USERS.qualify("alice", Optional.empty())).isEqualTo("alice");
        // an excluded operational label
        assertThat(USERS.qualify("alice", Optional.of("coordinator.tenants.example.com"))).isEqualTo("alice");
        // more than one label, or a label that is not a DNS label
        assertThat(USERS.qualify("alice", Optional.of("a.b.tenants.example.com"))).isEqualTo("alice");
        assertThat(USERS.qualify("alice", Optional.of("-a.tenants.example.com"))).isEqualTo("alice");
        assertThat(USERS.qualify("alice", Optional.of("a_b.tenants.example.com"))).isEqualTo("alice");
    }

    @Test
    void testNestedDomains()
    {
        HostQualifiedUsers users = new HostQualifiedUsers(ImmutableList.of("example.com", "tenants.example.com"), ImmutableSet.of());
        assertThat(users.qualify("alice", Optional.of("tenant-a.tenants.example.com"))).isEqualTo("tenant-a.alice");
        assertThat(users.qualify("alice", Optional.of("tenants.example.com"))).isEqualTo("tenants.alice");
    }

    @Test
    void testDisabled()
    {
        assertThat(HostQualifiedUsers.disabled().isEnabled()).isFalse();
        assertThat(HostQualifiedUsers.disabled().qualify("alice", Optional.of("tenant-a.tenants.example.com"))).isEqualTo("alice");
        assertThat(USERS.isEnabled()).isTrue();
    }

    @Test
    void testRejectsInvalidDomains()
    {
        assertThatThrownBy(() -> new HostQualifiedUsers(ImmutableList.of(""), ImmutableSet.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HostQualifiedUsers(ImmutableList.of("tenants..example.com"), ImmutableSet.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new HostQualifiedUsers(ImmutableList.of("*.example.com"), ImmutableSet.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new HostQualifiedUsers(ImmutableList.of(".tenants.example.com."), ImmutableSet.of())
                .qualify("alice", Optional.of("tenant-a.tenants.example.com")))
                .isEqualTo("tenant-a.alice");
    }
}

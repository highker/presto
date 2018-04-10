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
package com.facebook.presto.hive.metastore;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import javax.annotation.concurrent.Immutable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

@Immutable
public class PrincipalPrivileges
{
    private final SetMultimap<String, HivePrivilegeInfo> userPrivileges;
    private final SetMultimap<String, HivePrivilegeInfo> rolePrivileges;

    public PrincipalPrivileges(
            @JsonProperty("userPrivileges") Multimap<String, HivePrivilegeInfo> userPrivileges,
            @JsonProperty("rolePrivileges") Multimap<String, HivePrivilegeInfo> rolePrivileges)
    {
        this.userPrivileges = ImmutableSetMultimap.copyOf(requireNonNull(userPrivileges, "userPrivileges is null"));
        this.rolePrivileges = ImmutableSetMultimap.copyOf(requireNonNull(rolePrivileges, "rolePrivileges is null"));
    }

    @JsonProperty
    public SetMultimap<String, HivePrivilegeInfo> getUserPrivileges()
    {
        return userPrivileges;
    }

    @JsonProperty
    public SetMultimap<String, HivePrivilegeInfo> getRolePrivileges()
    {
        return rolePrivileges;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        PrincipalPrivileges otherPrivileges = (PrincipalPrivileges) other;
        return Objects.equals(userPrivileges, otherPrivileges.userPrivileges) &&
                Objects.equals(rolePrivileges, otherPrivileges.rolePrivileges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(userPrivileges, rolePrivileges);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(PrincipalPrivileges table)
    {
        return new Builder(table);
    }

    public static class Builder
    {
        private Multimap<String, HivePrivilegeInfo> userPrivileges = ArrayListMultimap.create();
        private Multimap<String, HivePrivilegeInfo> rolePrivileges = ArrayListMultimap.create();

        public Builder()
        {
        }

        public Builder(PrincipalPrivileges principalPrivileges)
        {
            userPrivileges.putAll(principalPrivileges.getUserPrivileges());
            rolePrivileges.putAll(principalPrivileges.getRolePrivileges());
        }

        public Builder setUserPrivileges(Multimap<String, HivePrivilegeInfo> userPrivileges)
        {
            this.userPrivileges = ArrayListMultimap.create(userPrivileges);
            return this;
        }

        public Builder addUserPriviledge(String userName, HivePrivilegeInfo privilege)
        {
            userPrivileges.put(userName, privilege);
            return this;
        }

        public Builder setRolePrivileges(Multimap<String, HivePrivilegeInfo> rolePrivileges)
        {
            this.rolePrivileges = ArrayListMultimap.create(rolePrivileges);
            return this;
        }

        public Builder addRolePriviledge(String roleName, HivePrivilegeInfo privilege)
        {
            rolePrivileges.put(roleName, privilege);
            return this;
        }
    }
}

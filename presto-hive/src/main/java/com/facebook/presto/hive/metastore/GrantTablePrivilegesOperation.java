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

import com.facebook.presto.hive.HdfsEnvironment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jdk.nashorn.internal.ir.annotations.Immutable;

import java.util.Objects;
import java.util.Set;

@Immutable
public class GrantTablePrivilegesOperation
         implements ExclusiveOperation
{
    private final String databaseName;
    private final String tableName;
    private final String grantee;
    private final Set<HivePrivilegeInfo> privileges;

    @JsonCreator
    public GrantTablePrivilegesOperation(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("grantee") String grantee,
            @JsonProperty("privileges") Set<HivePrivilegeInfo> privileges)
    {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.grantee = grantee;
        this.privileges = privileges;
    }

    @JsonProperty
    public String getDatabaseName()
    {
        return databaseName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getGrantee()
    {
        return grantee;
    }

    @JsonProperty
    public Set<HivePrivilegeInfo> getPrivileges()
    {
        return privileges;
    }

    @Override
    public void execute(ExtendedHiveMetastore delegate, HdfsEnvironment hdfsEnvironment)
    {
        delegate.grantTablePrivileges(databaseName, tableName, grantee, privileges);
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
        GrantTablePrivilegesOperation otherOperation = (GrantTablePrivilegesOperation) other;
        return Objects.equals(databaseName, otherOperation.databaseName) &&
                Objects.equals(tableName, otherOperation.tableName) &&
                Objects.equals(grantee, otherOperation.grantee) &&
                Objects.equals(privileges, otherOperation.privileges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, tableName, grantee, privileges);
    }
}

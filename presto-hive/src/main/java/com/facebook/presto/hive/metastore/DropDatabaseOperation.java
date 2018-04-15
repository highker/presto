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

@Immutable
public class DropDatabaseOperation
         implements ExclusiveOperation
{
    private final String schemaName;

    @JsonCreator
    public DropDatabaseOperation(@JsonProperty("schemaName") String schemaName)
    {
        this.schemaName = schemaName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public void execute(ExtendedHiveMetastore delegate, HdfsEnvironment hdfsEnvironment)
    {
        delegate.dropDatabase(schemaName);
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
        DropDatabaseOperation otherOperation = (DropDatabaseOperation) other;
        return Objects.equals(schemaName, otherOperation.schemaName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName);
    }
}

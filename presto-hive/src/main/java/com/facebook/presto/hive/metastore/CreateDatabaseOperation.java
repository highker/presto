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
public class CreateDatabaseOperation
         implements ExclusiveOperation
{
    private final Database database;

    @JsonCreator
    public CreateDatabaseOperation(@JsonProperty("database") Database database)
    {
        this.database = database;
    }

    @JsonProperty
    public Database getDatabase()
    {
        return database;
    }

    @Override
    public void execute(ExtendedHiveMetastore delegate, HdfsEnvironment hdfsEnvironment)
    {
        delegate.createDatabase(database);
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
        CreateDatabaseOperation otherOperation = (CreateDatabaseOperation) other;
        return Objects.equals(database, otherOperation.database);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(database);
    }
}

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
import com.facebook.presto.hive.HiveType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jdk.nashorn.internal.ir.annotations.Immutable;

import java.util.Objects;

@Immutable
public class AddColumnOperation
         implements ExclusiveOperation
{
    private final String databaseName;
    private final String tableName;
    private final String columnName;
    private final HiveType columnType;
    private final String columnComment;

    @JsonCreator
    public AddColumnOperation(
            @JsonProperty("databaseName") String databaseName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") HiveType columnType,
            @JsonProperty("columnComment") String columnComment)
    {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
        this.columnComment = columnComment;
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
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public HiveType getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public String getColumnComment()
    {
        return columnComment;
    }

    @Override
    public void execute(ExtendedHiveMetastore delegate, HdfsEnvironment hdfsEnvironment)
    {
        delegate.addColumn(databaseName, tableName, columnName, columnType, columnComment);
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
        AddColumnOperation otherOperation = (AddColumnOperation) other;
        return Objects.equals(databaseName, otherOperation.databaseName) &&
                Objects.equals(tableName, otherOperation.tableName) &&
                Objects.equals(columnName, otherOperation.columnName) &&
                Objects.equals(columnType, otherOperation.columnType) &&
                Objects.equals(columnComment, otherOperation.columnComment);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(databaseName, tableName, columnName, columnType, columnComment);
    }
}

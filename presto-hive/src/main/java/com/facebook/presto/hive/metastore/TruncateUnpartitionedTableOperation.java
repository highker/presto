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
import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.hadoop.fs.Path;

import java.util.Objects;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.metastore.MetastoreUtil.recursiveDeleteFiles;
import static java.lang.String.format;

@Immutable
public class TruncateUnpartitionedTableOperation
         implements ExclusiveOperation
{
    private final SchemaTableName schemaTableName;
    private final String location;
    private final HdfsContext context;

    @JsonCreator
    public TruncateUnpartitionedTableOperation(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("location") String location,
            @JsonProperty("context") HdfsContext context)
    {
        this.schemaTableName = schemaTableName;
        this.location = location;
        this.context = context;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public HdfsContext getContext()
    {
        return context;
    }

    @Override
    public void execute(ExtendedHiveMetastore delegate, HdfsEnvironment hdfsEnvironment)
    {
        RecursiveDeleteResult recursiveDeleteResult = recursiveDeleteFiles(hdfsEnvironment, context, new Path(location), ImmutableList.of(""), false);
        if (!recursiveDeleteResult.getNotDeletedEligibleItems().isEmpty()) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, format(
                    "Error deleting from unpartitioned table %s. These items can not be deleted: %s",
                    schemaTableName,
                    recursiveDeleteResult.getNotDeletedEligibleItems()));
        }
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
        TruncateUnpartitionedTableOperation otherOperation = (TruncateUnpartitionedTableOperation) other;
        return Objects.equals(schemaTableName, otherOperation.schemaTableName) &&
                Objects.equals(location, otherOperation.location) &&
                Objects.equals(context, otherOperation.context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName, location, context);
    }
}

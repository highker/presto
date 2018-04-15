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
public class RenameDatabaseOperation
         implements ExclusiveOperation
{
    private final String source;
    private final String target;

    @JsonCreator
    public RenameDatabaseOperation(@JsonProperty("source") String source, @JsonProperty("target") String target)
    {
        this.source = source;
        this.target = target;
    }

    @JsonProperty
    public String getSource()
    {
        return source;
    }

    @JsonProperty
    public String getTarget()
    {
        return target;
    }

    @Override
    public void execute(ExtendedHiveMetastore delegate, HdfsEnvironment hdfsEnvironment)
    {
        delegate.renameDatabase(source, target);
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
        RenameDatabaseOperation otherOperation = (RenameDatabaseOperation) other;
        return Objects.equals(source, otherOperation.source) && Objects.equals(target, otherOperation.target);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, target);
    }
}

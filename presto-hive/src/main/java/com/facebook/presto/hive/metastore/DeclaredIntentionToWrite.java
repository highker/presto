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

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class DeclaredIntentionToWrite
{
    private final WriteMode mode;
    private final HdfsContext context;
    private final String filePrefix;
    private final Path rootPath;
    private final SchemaTableName schemaTableName;

    public DeclaredIntentionToWrite(WriteMode mode, HdfsContext context, Path stagingPathRoot, String filePrefix, SchemaTableName schemaTableName)
    {
        this.mode = requireNonNull(mode, "mode is null");
        this.context = requireNonNull(context, "context is null");
        this.rootPath = requireNonNull(stagingPathRoot, "stagingPathRoot is null");
        this.filePrefix = requireNonNull(filePrefix, "filePrefix is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
    }

    @JsonCreator
    public DeclaredIntentionToWrite(
            @JsonProperty("mode") WriteMode mode,
            @JsonProperty("context") HdfsContext context,
            @JsonProperty("rootPathUri") URI rootPathUri,
            @JsonProperty("filePrefix") String filePrefix,
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName)
    {
        this(mode, context, new Path(requireNonNull(rootPathUri, "rootPathUri is null")), filePrefix, schemaTableName);
    }

    @JsonProperty
    public WriteMode getMode()
    {
        return mode;
    }

    @JsonProperty
    public HdfsContext getContext()
    {
        return context;
    }

    @JsonProperty
    public String getFilePrefix()
    {
        return filePrefix;
    }

    public Path getRootPath()
    {
        return rootPath;
    }

    @JsonProperty
    public URI getRootPathUri()
    {
        return rootPath.toUri();
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
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
        DeclaredIntentionToWrite otherDeclaredIntentionToWrite = (DeclaredIntentionToWrite) other;
        return Objects.equals(mode, otherDeclaredIntentionToWrite.mode) &&
                Objects.equals(context, otherDeclaredIntentionToWrite.context) &&
                Objects.equals(filePrefix, otherDeclaredIntentionToWrite.filePrefix) &&
                Objects.equals(rootPath, otherDeclaredIntentionToWrite.rootPath) &&
                Objects.equals(schemaTableName, otherDeclaredIntentionToWrite.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(mode, context, filePrefix, rootPath, schemaTableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("mode", mode)
                .add("context", context)
                .add("filePrefix", filePrefix)
                .add("rootPath", rootPath)
                .add("schemaTableName", schemaTableName)
                .toString();
    }
}

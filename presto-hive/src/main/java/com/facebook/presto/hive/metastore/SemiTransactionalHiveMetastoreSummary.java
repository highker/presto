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

import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.airlift.json.ObjectMapperProvider;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

@Immutable
@JsonDeserialize(using = SemiTransactionalHiveMetastoreSummary.SummaryDeserializer.class)
public class SemiTransactionalHiveMetastoreSummary
{
    private static final Map<String, Class<? extends ExclusiveOperation>> EXCLUSIVE_OPERATIONS;

    static {
        ImmutableMap.Builder<String, Class<? extends ExclusiveOperation>> builder = ImmutableMap.builder();
        builder.put(AddColumnOperation.class.getSimpleName(), AddColumnOperation.class);
        builder.put(CreateDatabaseOperation.class.getSimpleName(), CreateDatabaseOperation.class);
        builder.put(DropColumnOperation.class.getSimpleName(), DropColumnOperation.class);
        builder.put(DropDatabaseOperation.class.getSimpleName(), DropDatabaseOperation.class);
        builder.put(GrantTablePrivilegesOperation.class.getSimpleName(), GrantTablePrivilegesOperation.class);
        builder.put(RenameColumnOperation.class.getSimpleName(), RenameColumnOperation.class);
        builder.put(RenameDatabaseOperation.class.getSimpleName(), RenameDatabaseOperation.class);
        builder.put(RenameTableOperation.class.getSimpleName(), RenameTableOperation.class);
        builder.put(ReplaceTableOperation.class.getSimpleName(), ReplaceTableOperation.class);
        builder.put(RevokeTablePrivilegesOperation.class.getSimpleName(), RevokeTablePrivilegesOperation.class);
        builder.put(TruncateUnpartitionedTableOperation.class.getSimpleName(), TruncateUnpartitionedTableOperation.class);
        EXCLUSIVE_OPERATIONS = builder.build();
    }

    private final Map<SchemaTableName, Action<TableAndMore>> tableActions;
    private final Map<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> partitionActions;
    private final List<DeclaredIntentionToWrite> declaredIntentionsToWrite;
    private final ExclusiveOperation bufferedExclusiveOperation;
    private final String ExclusiveOperationClassName;
    private final State state;
    private final boolean throwOnCleanupFailure;

    public SemiTransactionalHiveMetastoreSummary(
            Map<SchemaTableName, Action<TableAndMore>> tableActions,
            Map<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> partitionActions,
            List<DeclaredIntentionToWrite> declaredIntentionsToWrite,
            ExclusiveOperation bufferedExclusiveOperation,
            State state,
            boolean throwOnCleanupFailure)
    {
        this.tableActions = tableActions;
        this.partitionActions = partitionActions;
        this.declaredIntentionsToWrite = declaredIntentionsToWrite;
        this.bufferedExclusiveOperation = bufferedExclusiveOperation;
        this.ExclusiveOperationClassName = bufferedExclusiveOperation == null ? null : bufferedExclusiveOperation.getClass().getSimpleName();
        this.state = state;
        this.throwOnCleanupFailure = throwOnCleanupFailure;
    }

    @JsonProperty
    public Map<SchemaTableName, Action<TableAndMore>> getTableActions()
    {
        return tableActions;
    }

    @JsonProperty
    public Map<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> getPartitionActions()
    {
        return partitionActions;
    }

    @JsonProperty
    public List<DeclaredIntentionToWrite> getDeclaredIntentionsToWrite()
    {
        return declaredIntentionsToWrite;
    }

    @JsonProperty
    public ExclusiveOperation getBufferedExclusiveOperation()
    {
        return bufferedExclusiveOperation;
    }

    @JsonProperty
    public String getExclusiveOperationClassName()
    {
        return ExclusiveOperationClassName;
    }

    @JsonProperty
    public State getState()
    {
        return state;
    }

    @JsonProperty
    public boolean isThrowOnCleanupFailure()
    {
        return throwOnCleanupFailure;
    }

    public static class SummaryDeserializer
            extends JsonDeserializer<SemiTransactionalHiveMetastoreSummary>
    {
        private static final ObjectMapper MAPPER = new ObjectMapperProvider().get();
        private static final JavaType TABLE_ACTIONS_TYPE = MAPPER.getTypeFactory().constructType(new TypeToken<Map<SchemaTableName, Action<TableAndMore>>>() {}.getType());

        @Override
        public SemiTransactionalHiveMetastoreSummary deserialize(JsonParser jsonParser, DeserializationContext context)
                throws IOException
        {
            JsonNode node = jsonParser.getCodec().readTree(jsonParser);

            Map<SchemaTableName, Action<TableAndMore>> tableActions = MAPPER.readValue(MAPPER.treeAsTokens(node.get("tableActions")), TABLE_ACTIONS_TYPE);

            Map<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> partitionActions = MAPPER.readValue(
                    MAPPER.treeAsTokens(node.get("partitionActions")),
                    new TypeReference<Map<SchemaTableName, Map<List<String>, Action<PartitionAndMore>>> >() {});

            List<DeclaredIntentionToWrite> declaredIntentionsToWrite = MAPPER.readValue(
                    MAPPER.treeAsTokens(node.get("declaredIntentionsToWrite")),
                    new TypeReference<List<DeclaredIntentionToWrite>>() {});

            String exclusiveOperationClassName = MAPPER.readValue("ExclusiveOperationClassName", String.class);
            Class<? extends ExclusiveOperation> exclusiveOperationClass = EXCLUSIVE_OPERATIONS.get(exclusiveOperationClassName);
            if (exclusiveOperationClass == null) {
                throw new UnsupportedOperationException(format("Unsupported ExclusiveOperation [%s]", exclusiveOperationClassName));
            }
            ExclusiveOperation bufferedExclusiveOperation = MAPPER.readValue(MAPPER.treeAsTokens(node.get("bufferedExclusiveOperation")), exclusiveOperationClass);

            State state = MAPPER.readValue("state", State.class);
            boolean throwOnCleanupFailure = MAPPER.readValue("throwOnCleanupFailure", Boolean.class);

            return new SemiTransactionalHiveMetastoreSummary(
                    tableActions,
                    partitionActions,
                    declaredIntentionsToWrite,
                    bufferedExclusiveOperation,
                    state,
                    throwOnCleanupFailure);
        }
    }
}

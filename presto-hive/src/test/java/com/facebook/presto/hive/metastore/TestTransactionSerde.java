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
import com.facebook.presto.spi.security.BasicPrincipal;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.reflect.TypeToken;
import io.airlift.json.JsonCodec;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestTransactionSerde
{
    @Test
    public void testTableAndMoreSerde()
    {
        JsonCodec<TableAndMore> codec = jsonCodec(TableAndMore.class);

        TableAndMore tableAndMore = new TableAndMore(
                table(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false);

        assertRoundTrip(codec, tableAndMore);

        tableAndMore = new TableAndMore(
                table(),
                Optional.of(principalPrivileges()),
                Optional.of(new Path("hdfs://VOL1:9000/db_name/table_name")),
                Optional.of(ImmutableList.of("file1", "file2")),
                false);

        assertRoundTrip(codec, tableAndMore);
    }

    @Test
    public void testPartitionAndMoreSerde()
    {
        JsonCodec<PartitionAndMore> codec = jsonCodec(PartitionAndMore.class);

        PartitionAndMore partitionAndMore = new PartitionAndMore(
                partition(),
                new Path("hdfs://VOL1:9000/db_name/table_name"),
                Optional.empty());

        assertRoundTrip(codec, partitionAndMore);

        partitionAndMore = new PartitionAndMore(
                partition(),
                new Path("hdfs://VOL1:9000/db_name/table_name"),
                Optional.of(ImmutableList.of("file1")));

        assertRoundTrip(codec, partitionAndMore);
    }

    @Test
    public void testActionSerde()
    {
        // no data
        JsonCodec<Action> codecNothing = jsonCodec(Action.class);
        Action action = new Action<>(ActionType.DROP, null, context());
        assertRoundTrip(codecNothing, action);

        // TableAndMore as data
        JsonCodec<Action<TableAndMore>> codecTableAndMore = jsonCodec(new TypeToken<Action<TableAndMore>>() {});
        Action<TableAndMore> tableAndMoreAction = new Action<>(
                ActionType.ADD,
                new TableAndMore(table(), Optional.empty(), Optional.empty(), Optional.empty(), false),
                context());
        assertRoundTrip(codecTableAndMore, tableAndMoreAction);

        // PartitionAndMore as data
        JsonCodec<Action<PartitionAndMore>> codecPartitionAndMore = jsonCodec(new TypeToken<Action<PartitionAndMore>>() {});
        Action<PartitionAndMore> partitionAndMoreAction = new Action<>(
                ActionType.ADD,
                new PartitionAndMore(partition(), new Path("hdfs://VOL1:9000/db_name/table_name"), Optional.empty()),
                context());
        assertRoundTrip(codecPartitionAndMore, partitionAndMoreAction);
    }

    private <T> void assertRoundTrip(JsonCodec<T> codec, T object)
    {
        assertEquals(codec.fromJson(codec.toJson(object)), object);
    }

    private static Table table()
    {
        Table.Builder tableBuilder = Table.builder();
        tableBuilder.getStorageBuilder()
                .setStorageFormat(
                        StorageFormat.create(
                                "com.facebook.hive.orc.OrcSerde",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
                .setLocation("hdfs://VOL1:9000/db_name/table_name")
                .setSkewed(false)
                .setSorted(false);

        return tableBuilder
                .setDatabaseName("test_dbname")
                .setOwner("testOwner")
                .setTableName("test_table")
                .setTableType(TableType.MANAGED_TABLE.toString())
                .setDataColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(ImmutableMap.of())
                .setPartitionColumns(ImmutableList.of(new Column("col2", HIVE_STRING, Optional.empty())))
                .build();
    }

    private static PrincipalPrivileges principalPrivileges()
    {
        Table table = table();
        return new PrincipalPrivileges(
                ImmutableMultimap.<String, HivePrivilegeInfo>builder()
                        .put(table.getOwner(), new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.SELECT, true))
                        .put(table.getOwner(), new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.INSERT, true))
                        .put(table.getOwner(), new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.UPDATE, true))
                        .put(table.getOwner(), new HivePrivilegeInfo(HivePrivilegeInfo.HivePrivilege.DELETE, true))
                        .build(),
                ImmutableMultimap.of());
    }

    private static Partition partition()
    {
        Partition.Builder partitionBuilder = Partition.builder();
        partitionBuilder.getStorageBuilder()
                .setStorageFormat(
                        StorageFormat.create(
                                "com.facebook.hive.orc.OrcSerde",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                                "org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
                .setLocation("hdfs://VOL1:9000/db_name/table_name")
                .setSkewed(false)
                .setSorted(false);

        return partitionBuilder
                .setDatabaseName("test_dbname")
                .setTableName("test_table")
                .setValues(ImmutableList.of("value1", "value2"))
                .setColumns(ImmutableList.of(new Column("col1", HIVE_STRING, Optional.empty())))
                .setParameters(ImmutableMap.of("param1", "value1"))
                .build();
    }

    private static HdfsContext context()
    {
        return new HdfsContext(
                TestingSession.testSessionBuilder()
                        .setIdentity(new Identity("user", Optional.of(new BasicPrincipal("principal"))))
                        .build()
                        .toConnectorSession(),
                table().getDatabaseName(),
                table().getTableName());
    }
}

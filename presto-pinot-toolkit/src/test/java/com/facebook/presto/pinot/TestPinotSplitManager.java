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
package com.facebook.presto.pinot;

import com.facebook.presto.pinot.query.PinotQueryGenerator;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.pinot.PinotSplit.SplitType.BROKER;
import static com.facebook.presto.pinot.PinotSplit.SplitType.SEGMENT;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestPinotSplitManager
{
    // Test table and related info
    public static ConnectorId pinotConnectorId = new ConnectorId("id");
    public static PinotTableHandle realtimeOnlyTable = new PinotTableHandle(pinotConnectorId.getCatalogName(), "schema", "realtimeOnly");
    public static PinotTableHandle hybridTable = new PinotTableHandle(pinotConnectorId.getCatalogName(), "schema", "hybrid");
    public static PinotColumnHandle regionId = new PinotColumnHandle("regionId", BIGINT, REGULAR);
    public static PinotColumnHandle city = new PinotColumnHandle("city", VARCHAR, REGULAR);

    private final PinotConfig pinotConfig = new PinotConfig();
    private final PinotConnection pinotConnection = new PinotConnection(new MockPinotClusterInfoFetcher(pinotConfig), pinotConfig, Executors.newSingleThreadExecutor());
    private final PinotSplitManager pinotSplitManager = new PinotSplitManager(pinotConnectorId, pinotConnection);

    @Test
    public void testRealtimeSegmentSplitsOneSegmentPerServer()
    {
        testSegmentSplitsHelper(realtimeOnlyTable, 1, 4);  // 2 servers with 2 segments each
    }

    private void testSegmentSplitsHelper(PinotTableHandle table, int segmentsPerSplit, int expectedNumSplits)
    {
        PinotQueryGenerator.GeneratedPql generatedPql = new PinotQueryGenerator.GeneratedPql(table.getTableName(), String.format("SELECT %s, %s FROM %s LIMIT %d", city.getColumnName(), regionId.getColumnName(), table.getTableName(), pinotConfig.getLimitLargeForSegment()), ImmutableList.of(0, 1), 0, false, false);
        PinotTableHandle pinotTableHandle = new PinotTableHandle(table.getConnectorId(), table.getSchemaName(), table.getTableName(), Optional.of(false), Optional.of(generatedPql));
        List<PinotSplit> splits = getSplitsHelper(pinotTableHandle, segmentsPerSplit, false);
        assertSplits(splits, expectedNumSplits, SEGMENT);
    }

    @Test
    public void testSplitsBroker()
    {
        PinotQueryGenerator.GeneratedPql generatedPql = new PinotQueryGenerator.GeneratedPql(realtimeOnlyTable.getTableName(), String.format("SELECT %s, COUNT(1) FROM %s GROUP BY %s TOP %d", city.getColumnName(), realtimeOnlyTable.getTableName(), city.getColumnName(), pinotConfig.getTopNLarge()), ImmutableList.of(0, 1), 1, false, true);
        PinotTableHandle pinotTableHandle = new PinotTableHandle(realtimeOnlyTable.getConnectorId(), realtimeOnlyTable.getSchemaName(), realtimeOnlyTable.getTableName(), Optional.of(true), Optional.of(generatedPql));
        List<PinotSplit> splits = getSplitsHelper(pinotTableHandle, 1, false);
        assertSplits(splits, 1, BROKER);
    }

    @Test(expectedExceptions = PinotSplitManager.QueryNotAdequatelyPushedDownException.class)
    public void testBrokerNonShortQuery()
    {
        PinotQueryGenerator.GeneratedPql generatedPql = new PinotQueryGenerator.GeneratedPql(realtimeOnlyTable.getTableName(), String.format("SELECT %s FROM %s", city.getColumnName(), realtimeOnlyTable.getTableName()), ImmutableList.of(0), 0, false, false);
        PinotTableHandle pinotTableHandle = new PinotTableHandle(realtimeOnlyTable.getConnectorId(), realtimeOnlyTable.getSchemaName(), realtimeOnlyTable.getTableName(), Optional.of(false), Optional.of(generatedPql));
        List<PinotSplit> splits = getSplitsHelper(pinotTableHandle, 1, true);
        assertSplits(splits, 1, BROKER);
    }

    @Test
    public void testRealtimeSegmentSplitsManySegmentPerServer()
    {
        testSegmentSplitsHelper(realtimeOnlyTable, Integer.MAX_VALUE, 2);
    }

    @Test
    public void testHybridSegmentSplitsOneSegmentPerServer()
    {
        testSegmentSplitsHelper(hybridTable, 1, 8);
    }

    private void assertSplits(List<PinotSplit> splits, int numSplitsExpected, PinotSplit.SplitType segment)
    {
        assertEquals(splits.size(), numSplitsExpected);
        splits.forEach(s -> assertEquals(s.getSplitType(), segment));
    }

    public static ConnectorSession createSessionWithNumSplits(int numSegmentsPerSplit, boolean forbidSegmentQueries, PinotConfig pinotConfig)
    {
        return new TestingConnectorSession(
                "user",
                Optional.of("test"),
                Optional.empty(),
                UTC_KEY,
                ENGLISH,
                System.currentTimeMillis(),
                new PinotSessionProperties(pinotConfig).getSessionProperties(),
                ImmutableMap.of(PinotSessionProperties.NUM_SEGMENTS_PER_SPLIT, numSegmentsPerSplit,
                        PinotSessionProperties.FORBID_SEGMENT_QUERIES, forbidSegmentQueries),
                new FeaturesConfig().isLegacyTimestamp(),
                Optional.empty());
    }

    private List<PinotSplit> getSplitsHelper(PinotTableHandle pinotTable, int numSegmentsPerSplit, boolean forbidSegmentQueries)
    {
        PinotTableLayoutHandle pinotTableLayout = new PinotTableLayoutHandle(pinotTable);
        ConnectorSession session = createSessionWithNumSplits(numSegmentsPerSplit, forbidSegmentQueries, pinotConfig);
        ConnectorSplitSource splitSource = pinotSplitManager.getSplits(null, session, pinotTableLayout, null);
        List<PinotSplit> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits().stream().map(s -> (PinotSplit) s).collect(toList()));
        }

        return splits;
    }
}

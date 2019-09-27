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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestPinotQueryGenerator
        extends TestPinotQueryBase
{
    private static final SessionHolder defaultSessionHolder = new SessionHolder(false);

    private PlanBuilder createPb(SessionHolder sessionHolder)
    {
        return new PlanBuilder(sessionHolder.getSession(), new PlanNodeIdAllocator(), metadata);
    }

    private void testPQL(
            PinotConfig givenPinotConfig,
            Function<PlanBuilder, PlanNode> planBuilderConsumer,
            String expectedPQL, SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        PlanNode planNode = planBuilderConsumer.apply(createPb(sessionHolder));
        testPQL(givenPinotConfig, planNode, expectedPQL, sessionHolder, outputVariables);
    }

    private void testPQL(
            PinotConfig givenPinotConfig,
            PlanNode planNode,
            String expectedPQL,
            SessionHolder sessionHolder,
            Map<String, String> outputVariables)
    {
        PinotQueryGenerator.PinotQueryGeneratorResult pinotQueryGeneratorResult = new PinotQueryGenerator(givenPinotConfig, typeManager, functionMetadataManager, standardFunctionResolution).generate(planNode, sessionHolder.getConnectorSession()).get();
        if (expectedPQL.contains("__expressions__")) {
            String expressions = planNode.getOutputVariables().stream().map(v -> outputVariables.get(v.getName())).filter(v -> v != null).collect(Collectors.joining(", "));
            expectedPQL = expectedPQL.replace("__expressions__", expressions);
        }
        assertEquals(pinotQueryGeneratorResult.getGeneratedPql().getPql(), expectedPQL);
    }

    private void testPQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPQL, SessionHolder sessionHolder, Map<String, String> outputVariables)
    {
        testPQL(pinotConfig, planBuilderConsumer, expectedPQL, sessionHolder, outputVariables);
    }

    private void testPQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPQL, SessionHolder sessionHolder)
    {
        testPQL(planBuilderConsumer, expectedPQL, sessionHolder, ImmutableMap.of());
    }

    private void testPQL(Function<PlanBuilder, PlanNode> planBuilderConsumer, String expectedPQL)
    {
        testPQL(planBuilderConsumer, expectedPQL, defaultSessionHolder);
    }

    private PlanNode buildPlan(Function<PlanBuilder, PlanNode> consumer)
    {
        PlanBuilder pb = createPb(defaultSessionHolder);
        return consumer.apply(pb);
    }

    private void testUnaryAggregationHelper(BiConsumer<PlanBuilder, PlanBuilder.AggregationBuilder> aggregationFunctionBuilder, String expectedAggOutput)
    {
        PlanNode justScan = buildPlan(pb -> tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare));
        PlanNode filter = buildPlan(pb -> filter(pb, tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("fare > 3", defaultSessionHolder)));
        PlanNode anotherFilter = buildPlan(pb -> filter(pb, tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare), getRowExpression("secondssinceepoch between 200 and 300 and regionid >= 40", defaultSessionHolder)));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(pb, aggBuilder.source(justScan).globalGrouping())),
                format("SELECT %s FROM realtimeOnly", expectedAggOutput));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(pb, aggBuilder.source(filter).globalGrouping())),
                format("SELECT %s FROM realtimeOnly WHERE (fare > 3)", expectedAggOutput));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(pb, aggBuilder.source(filter).singleGroupingSet(v("regionid")))),
                format("SELECT %s FROM realtimeOnly WHERE (fare > 3) GROUP BY regionId TOP 10000", expectedAggOutput));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(pb, aggBuilder.source(justScan).singleGroupingSet(v("regionid")))),
                format("SELECT %s FROM realtimeOnly GROUP BY regionId TOP 10000", expectedAggOutput));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggregationFunctionBuilder.accept(pb, aggBuilder.source(anotherFilter).singleGroupingSet(v("regionid"), v("city")))),
                format("SELECT %s FROM realtimeOnly WHERE ((secondsSinceEpoch BETWEEN 200 AND 300) AND (regionId >= 40)) GROUP BY regionId, city TOP 10000", expectedAggOutput));
    }

    @Test
    public void testSimpleSelectStar()
    {
        testPQL(pb -> limit(pb, 50L, tableScan(pb, pinotTable, regionId, city, fare, secondsSinceEpoch)),
                "SELECT regionId, city, fare, secondsSinceEpoch FROM realtimeOnly LIMIT 50");
        testPQL(pb -> limit(pb, 50L, tableScan(pb, pinotTable, regionId, secondsSinceEpoch)),
                "SELECT regionId, secondsSinceEpoch FROM realtimeOnly LIMIT 50");
    }

    @Test
    public void testSimpleSelectWithFilterLimit()
    {
        testPQL(pb -> limit(pb, 50L, project(pb, filter(pb, tableScan(pb, pinotTable, regionId, city, fare, secondsSinceEpoch), getRowExpression("secondssinceepoch > 20", defaultSessionHolder)), ImmutableList.of("city", "secondssinceepoch"))),
                "SELECT city, secondsSinceEpoch FROM realtimeOnly WHERE (secondsSinceEpoch > 20) LIMIT 50");
    }

    @Test
    public void testCountStar()
    {
        testUnaryAggregationHelper((pb, aggregationBuilder) -> aggregationBuilder.addAggregation(pb.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)), "count(*)");
    }

    @Test
    public void testDistinctSelection()
    {
        PlanNode justScan = buildPlan(pb -> tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(v("regionid"))),
                "SELECT count(*) FROM realtimeOnly GROUP BY regionId TOP 10000");
    }

    @Test
    public void testPercentileAggregation()
    {
        testUnaryAggregationHelper((pb, aggregationBuilder) -> aggregationBuilder.addAggregation(pb.variable("agg"), getRowExpression("approx_percentile(fare, 0.10)", defaultSessionHolder)), "PERCENTILEEST10(fare)");
    }

    @Test
    public void testApproxDistinct()
    {
        testUnaryAggregationHelper((pb, aggregationBuilder) -> aggregationBuilder.addAggregation(pb.variable("agg"), getRowExpression("approx_distinct(fare)", defaultSessionHolder)), "DISTINCTCOUNTHLL(fare)");
    }

    @Test
    public void testAggWithUDFInGroupBy()
    {
        LinkedHashMap<String, String> aggProjection = new LinkedHashMap<>();
        aggProjection.put("date", "date_trunc('day', cast(from_unixtime(secondssinceepoch - 50) AS TIMESTAMP))");
        PlanNode justDate = buildPlan(pb -> project(pb, tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare), aggProjection, defaultSessionHolder));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggBuilder.source(justDate).singleGroupingSet(new VariableReferenceExpression("date", TIMESTAMP)).addAggregation(pb.variable("agg"), getRowExpression("count(*)", defaultSessionHolder))),
                "SELECT count(*) FROM realtimeOnly GROUP BY dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS') TOP 10000");
        aggProjection.put("city", "city");
        PlanNode newScanWithCity = buildPlan(pb -> project(pb, tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare), aggProjection, defaultSessionHolder));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggBuilder.source(newScanWithCity).singleGroupingSet(new VariableReferenceExpression("date", TIMESTAMP), v("city")).addAggregation(pb.variable("agg"), getRowExpression("count(*)", defaultSessionHolder))),
                "SELECT count(*) FROM realtimeOnly GROUP BY dateTimeConvert(SUB(secondsSinceEpoch, 50), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:DAYS'), city TOP 10000");
    }

    @Test
    public void testMultipleAggregatesWithOutGroupBy()
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(pb -> tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPQL(pb -> pb.aggregation(aggBuilder -> aggBuilder.source(justScan).globalGrouping().addAggregation(pb.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(pb.variable("min"), getRowExpression("min(fare)", defaultSessionHolder))),
                "SELECT __expressions__ FROM realtimeOnly", defaultSessionHolder, outputVariables);
        testPQL(pb -> pb.limit(50L, pb.aggregation(aggBuilder -> aggBuilder.source(justScan).globalGrouping().addAggregation(pb.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(pb.variable("min"), getRowExpression("min(fare)", defaultSessionHolder)))),
                "SELECT __expressions__ FROM realtimeOnly", defaultSessionHolder, outputVariables);
    }

    @Test
    public void testMultipleAggregatesWhenAllowed()
    {
        helperTestMultipleAggregatesWithGroupBy(new PinotConfig().setAllowMultipleAggregations(true));
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testMultipleAggregatesNotAllowed()
    {
        helperTestMultipleAggregatesWithGroupBy(pinotConfig);
    }

    private void helperTestMultipleAggregatesWithGroupBy(PinotConfig givenPinotConfig)
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(pb -> tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPQL(givenPinotConfig, pb -> pb.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(v("city")).addAggregation(pb.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(pb.variable("min"), getRowExpression("min(fare)", defaultSessionHolder))),
                "SELECT __expressions__ FROM realtimeOnly GROUP BY city TOP 10000", defaultSessionHolder, outputVariables);
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testMultipleAggregateGroupByWithLimitFails()
    {
        Map<String, String> outputVariables = ImmutableMap.of("agg", "count(*)", "min", "min(fare)");
        PlanNode justScan = buildPlan(pb -> tableScan(pb, pinotTable, regionId, secondsSinceEpoch, city, fare));
        testPQL(pb -> pb.limit(50L, pb.aggregation(aggBuilder -> aggBuilder.source(justScan).singleGroupingSet(v("city")).addAggregation(pb.variable("agg"), getRowExpression("count(*)", defaultSessionHolder)).addAggregation(pb.variable("min"), getRowExpression("min(fare)", defaultSessionHolder)))),
                "SELECT __expressions__ FROM realtimeOnly GROUP BY city TOP 50", defaultSessionHolder, outputVariables);
    }

    @Test
    public void testSimpleSelectWithTopN()
    {
        PlanBuilder pb = createPb(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(pb, pinotTable, regionId, city, fare);
        TopNNode topnFare = topn(pb, 50L, ImmutableList.of("fare"), ImmutableList.of(false), tableScanNode);
        testPQL(pinotConfig, topnFare,
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY fare DESC LIMIT 50", defaultSessionHolder, ImmutableMap.of());
        TopNNode topnFareAndCity = topn(pb, 50L, ImmutableList.of("fare", "city"), ImmutableList.of(true, false), tableScanNode);
        testPQL(pinotConfig, topnFareAndCity,
                "SELECT regionId, city, fare FROM realtimeOnly ORDER BY fare, city DESC LIMIT 50", defaultSessionHolder, ImmutableMap.of());
    }

    @Test(expectedExceptions = NoSuchElementException.class)
    public void testAggregationWithOrderByPushDownInTopN()
    {
        PlanBuilder pb = createPb(defaultSessionHolder);
        TableScanNode tableScanNode = tableScan(pb, pinotTable, city, fare);
        AggregationNode agg = pb.aggregation(aggBuilder -> aggBuilder.source(tableScanNode).singleGroupingSet(v("city")).addAggregation(pb.variable("agg"), getRowExpression("sum(fare)", defaultSessionHolder)));
        TopNNode topN = new TopNNode(pb.getIdAllocator().getNextId(), agg, 50L, new OrderingScheme(ImmutableList.of(new Ordering(v("city"), SortOrder.DESC_NULLS_FIRST))), TopNNode.Step.FINAL);
        testPQL(pinotConfig, topN, "", defaultSessionHolder, ImmutableMap.of());
    }
}

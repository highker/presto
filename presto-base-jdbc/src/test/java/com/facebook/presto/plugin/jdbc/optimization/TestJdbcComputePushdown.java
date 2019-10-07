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
package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcTableLayoutHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.optimization.function.OperatorTranslators;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.optimizations.connector.TranslatorAnnotationParser.parseFunctionTranslations;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TestJdbcComputePushdown
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final PlanBuilder PLAN_BUILDER = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), METADATA);
    private static final PlanNodeIdAllocator ID_ALLOCATOR = new PlanNodeIdAllocator();
    private static final String CATALOG_NAME = "Jdbc";
    private static final String CONNECTOR_ID = new ConnectorId(CATALOG_NAME).toString();

    private final TestingRowExpressionTranslator sqlToRowExpressionTranslator;

    private final JdbcComputePushdown jdbcComputePushdown;

    public TestJdbcComputePushdown()
    {
        this.sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(METADATA);
        FunctionManager functionManager = METADATA.getFunctionManager();
        StandardFunctionResolution functionResolution = new FunctionResolution(functionManager);
        DeterminismEvaluator determinismEvaluator = new RowExpressionDeterminismEvaluator(functionManager);
        ConnectorPlanOptimizerProvider.Context context = new ConnectorPlanOptimizerProvider.Context() {
            @Override
            public <T> Map<FunctionMetadata, ConnectorPlanOptimizerProvider.FunctionTranslator<T>> getFunctionTranslatorMapping(Class<T> expressionType)
            {
                return getFunctionTranslators().stream()
                        .map(containerClazz -> parseFunctionTranslations(expressionType, containerClazz))
                        .map(Map::entrySet)
                        .flatMap(Set::stream)
                        .collect(toMap(Map.Entry::getKey, entry -> (ConnectorPlanOptimizerProvider.FunctionTranslator<T>) translatedArguments -> {
                            try {
                                return Optional.ofNullable((T) entry.getValue().invokeWithArguments(translatedArguments));
                            }
                            catch (Throwable t) {
                                return Optional.empty();
                            }
                        }));
            }
        };

        this.jdbcComputePushdown = new JdbcComputePushdown(
                functionManager,
                functionResolution,
                determinismEvaluator,
                new RowExpressionOptimizer(METADATA),
                new RowExpressionToSqlTranslator(functionManager, context.getFunctionTranslatorMapping(JdbcSql.class), "'"));
    }

    @Test
    public void testJdbcComputePushdownAll()
    {
        String table = "test_table";
        String schema = "test_schema";

        String predicate = "(c1 + c2) - c2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(predicate), typeProvider);
        Set<ColumnHandle> columns = Arrays.asList("c1", "c2").stream().map(TestJdbcComputePushdown::intJdbcColumnHandle).collect(Collectors.toSet());
        PlanNode original = filter(jdbcTableScan(schema, table, "c1", "c2"), rowExpression);

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.none(), Optional.of(new JdbcSql("'c1' + 'c2' - 'c2'", null)));

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                predicate,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    @Test
    public void testJdbcComputePushdownUnsupported()
    {
        String table = "test_table";
        String schema = "test_schema";

        String predicate = "(c1 + c2) > c2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(predicate), typeProvider);
        Set<ColumnHandle> columns = Arrays.asList("c1", "c2").stream().map(TestJdbcComputePushdown::intJdbcColumnHandle).collect(Collectors.toSet());
        PlanNode original = filter(jdbcTableScan(schema, table, "c1", "c2"), rowExpression);

        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        // Test should expect an empty entry for translatedSql since > is an unsupported function currently in the optimizer
        JdbcTableLayoutHandle jdbcTableLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.none(), Optional.empty());

        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        PlanNode actual = this.jdbcComputePushdown.optimize(original, session, null, ID_ALLOCATOR);
        assertPlanMatch(actual, PlanMatchPattern.filter(
                predicate,
                JdbcTableScanMatcher.jdbcTableScanPattern(jdbcTableLayoutHandle, columns)));
    }

    private Set<Class<?>> getFunctionTranslators()
    {
        return ImmutableSet.of(OperatorTranslators.class);
    }

    private static VariableReferenceExpression newBigintVariable(String name)
    {
        return new VariableReferenceExpression(name, BIGINT);
    }

    private static JdbcColumnHandle intJdbcColumnHandle(String name)
    {
        return new JdbcColumnHandle(CONNECTOR_ID, name, new JdbcTypeHandle(Types.BIGINT, 10, 0), BIGINT, false);
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected)
    {
        assertPlanMatch(actual, expected, TypeProvider.empty());
    }

    private static void assertPlanMatch(PlanNode actual, PlanMatchPattern expected, TypeProvider typeProvider)
    {
        PlanAssert.assertPlan(
                TEST_SESSION,
                METADATA,
                (node, sourceStats, lookup, session, types) -> PlanNodeStatsEstimate.unknown(),
                new Plan(actual, typeProvider, StatsAndCosts.empty()),
                expected);
    }

    private TableScanNode jdbcTableScan(String schema, String table, String... columnNames)
    {
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(CONNECTOR_ID, new SchemaTableName(schema, table), CATALOG_NAME, schema, table);
        JdbcTableLayoutHandle cubrickTableLayoutHandle = new JdbcTableLayoutHandle(jdbcTableHandle, TupleDomain.none(), Optional.empty());
        TableHandle tableHandle = new TableHandle(new ConnectorId(CATALOG_NAME), jdbcTableHandle, new ConnectorTransactionHandle() {}, Optional.of(cubrickTableLayoutHandle));

        return PLAN_BUILDER.tableScan(
                tableHandle,
                Arrays.stream(columnNames).map(TestJdbcComputePushdown::newBigintVariable).collect(toImmutableList()),
                Arrays.stream(columnNames).map(TestJdbcComputePushdown::newBigintVariable).collect(toMap(identity(), entry -> intJdbcColumnHandle(entry.getName()))));
    }

    private FilterNode filter(PlanNode source, RowExpression predicate)
    {
        return PLAN_BUILDER.filter(predicate, source);
    }

    private static final class JdbcTableScanMatcher
            implements Matcher
    {
        private final JdbcTableLayoutHandle jdbcTableLayoutHandle;
        private final Set<ColumnHandle> columns;

        static PlanMatchPattern jdbcTableScanPattern(JdbcTableLayoutHandle jdbcTableLayoutHandle, Set<ColumnHandle> columns)
        {
            return node(TableScanNode.class).with(new JdbcTableScanMatcher(jdbcTableLayoutHandle, columns));
        }

        private JdbcTableScanMatcher(JdbcTableLayoutHandle jdbcTableLayoutHandle, Set<ColumnHandle> columns)
        {
            this.jdbcTableLayoutHandle = jdbcTableLayoutHandle;
            this.columns = columns;
        }

        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof TableScanNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

            TableScanNode tableScanNode = (TableScanNode) node;
            JdbcTableLayoutHandle layoutHandle = (JdbcTableLayoutHandle) tableScanNode.getTable().getLayout().get();
            if (jdbcTableLayoutHandle.getTable().equals(layoutHandle.getTable())
                    && jdbcTableLayoutHandle.getTupleDomain().equals(layoutHandle.getTupleDomain())
                    && ((!jdbcTableLayoutHandle.getTranslatedSql().isPresent() && !layoutHandle.getTranslatedSql().isPresent())
                        || jdbcTableLayoutHandle.getTranslatedSql().get().getSql().equals(layoutHandle.getTranslatedSql().get().getSql()))) {
                return MatchResult.match(
                        SymbolAliases.builder().putAll(
                        columns.stream()
                                .map(column -> ((JdbcColumnHandle) column).getColumnName())
                                .collect(toMap(identity(), SymbolReference::new)))
                        .build());
            }

            return MatchResult.NO_MATCH;
        }
    }
}

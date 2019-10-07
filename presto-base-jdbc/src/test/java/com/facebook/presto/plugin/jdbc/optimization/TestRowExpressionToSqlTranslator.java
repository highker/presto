package com.facebook.presto.plugin.jdbc.optimization;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.optimization.function.OperatorTranslators;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.relation.translator.FunctionTranslator;
import com.facebook.presto.spi.relation.translator.TranslatedExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.relation.translator.RowExpressionTreeTranslator.translateWith;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.optimizations.connector.TranslatorAnnotationParser.parseFunctionTranslations;
import static java.util.stream.Collectors.toMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionToSqlTranslator
{
    private static final Metadata METADATA = MetadataManager.createTestMetadataManager();
    private static final String CATALOG_NAME = "Jdbc";
    private static final String CONNECTOR_ID = new ConnectorId(CATALOG_NAME).toString();

    private final FunctionManager functionManager;
    private final StandardFunctionResolution functionResolution;
    private final DeterminismEvaluator determinismEvaluator;
    private final RowExpressionToSqlTranslator rowExpressionToSqlTranslator;
    private final TestingRowExpressionTranslator sqlToRowExpressionTranslator;

    public TestRowExpressionToSqlTranslator()
    {
        this.functionManager = METADATA.getFunctionManager();
        this.functionResolution = new FunctionResolution(this.functionManager);
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(this.functionManager);
        this.rowExpressionToSqlTranslator = new RowExpressionToSqlTranslator(functionManager, getFunctionTranslatorMapping(), "'");
        this.sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(METADATA);
    }

    @Test
    public void testBasicArithmeticTranslation()
    {
        String predicate = "(c1 + c2) - c2";
        TypeProvider typeProvider = TypeProvider.copyOf(ImmutableMap.of("c1", BIGINT, "c2", BIGINT));
        RowExpression rowExpression = sqlToRowExpressionTranslator.translateAndOptimize(expression(predicate), typeProvider);
        Map<VariableReferenceExpression, ColumnHandle> context = new ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle>()
                .put(new VariableReferenceExpression("c1", BIGINT), intJdbcColumnHandle("c1"))
                .put(new VariableReferenceExpression("c2", BIGINT), intJdbcColumnHandle("c2"))
                .build();
        TranslatedExpression<JdbcSql> jdbcSqlTranslatedExpression = translateWith(
                rowExpression,
                determinismEvaluator,
                functionResolution,
                functionManager,
                this.rowExpressionToSqlTranslator,
                context);
        assertTrue(jdbcSqlTranslatedExpression.getTranslated().isPresent());
        assertEquals(jdbcSqlTranslatedExpression.getTranslated().get().getSql(), "'c1' + 'c2' - 'c2'");
    }

    private static JdbcColumnHandle intJdbcColumnHandle(String name)
    {
        return new JdbcColumnHandle(CONNECTOR_ID, name, new JdbcTypeHandle(Types.BIGINT, 10, 0), BIGINT, false);
    }

    private Set<Class<?>> getFunctionTranslators()
    {
        return ImmutableSet.of(OperatorTranslators.class);
    }

    public <T> Map<FunctionMetadata, FunctionTranslator<T>> getFunctionTranslatorMapping()
    {
        return getFunctionTranslators().stream()
                .map(containerClazz -> parseFunctionTranslations(JdbcSql.class, containerClazz))
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(toMap(Map.Entry::getKey, entry -> (FunctionTranslator<T>) translatedArguments -> {
                    try {
                        return Optional.ofNullable((T) entry.getValue().invokeWithArguments(translatedArguments));
                    }
                    catch (Throwable t) {
                        return Optional.empty();
                    }
                }));
    }
}

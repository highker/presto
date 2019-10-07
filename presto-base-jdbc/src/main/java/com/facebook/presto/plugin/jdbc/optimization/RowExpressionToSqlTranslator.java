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

import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.relation.translator.FunctionTranslator;
import com.facebook.presto.spi.relation.translator.RowExpressionTranslator;
import com.facebook.presto.spi.relation.translator.TranslatedExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RowExpressionToSqlTranslator
        extends RowExpressionTranslator<JdbcSql, Map<VariableReferenceExpression, ColumnHandle>>
{
    private final FunctionMetadataManager functionMetadataManager;
    private final Map<FunctionMetadata, FunctionTranslator<JdbcSql>> functionTranslators;
    private final String quote;

    public RowExpressionToSqlTranslator(FunctionMetadataManager functionMetadataManager, Map<FunctionMetadata, FunctionTranslator<JdbcSql>> functionTranslators, String quote)
    {
        this.functionMetadataManager = functionMetadataManager;
        this.functionTranslators = functionTranslators;
        this.quote = quote;
    }

    @Override
    public Optional<JdbcSql> translateFunction(FunctionHandle functionHandle, List<TranslatedExpression<JdbcSql>> translatedExpressions, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(functionHandle);
        if (functionTranslators.containsKey(functionMetadata)) {
            if (translatedExpressions.stream().map(TranslatedExpression::getTranslated).allMatch(Optional::isPresent)) {
                return functionTranslators.get(functionMetadata).translate(translatedExpressions);
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<JdbcSql> translateConstant(ConstantExpression literal, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        if (isAcceptedType(literal.getType())) {
            return Optional.of(new JdbcSql("?", ImmutableList.of(literal)));
        }
        return Optional.empty();
    }

    @Override
    public Optional<JdbcSql> translateVariable(VariableReferenceExpression reference, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        JdbcColumnHandle columnHandle = (JdbcColumnHandle) context.get(reference);
        requireNonNull(columnHandle, format("Unrecognized variable %s", reference));
        return Optional.of(new JdbcSql(quote + columnHandle.getColumnName() + quote, ImmutableList.of()));
    }

    @Override
    public Optional<JdbcSql> translateSpecialForm(SpecialFormExpression.Form form, List<TranslatedExpression<JdbcSql>> translatedExpressions, Map<VariableReferenceExpression, ColumnHandle> context)
    {
        List<JdbcSql> jdbcSqls = translatedExpressions.stream()
                .map(TranslatedExpression::getTranslated)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        List<String> sqlBodies = jdbcSqls.stream()
                .map(JdbcSql::getSql)
                .map(sql -> '(' + sql + ')')
                .collect(toImmutableList());
        List<ConstantExpression> variableBindings = jdbcSqls.stream()
                .map(JdbcSql::getBindValues)
                .flatMap(List::stream)
                .collect(toImmutableList());
        if (jdbcSqls.size() < translatedExpressions.size()) {
            return Optional.empty();
        }
        switch (form) {
            case AND:
                return Optional.of(new JdbcSql(Joiner.on(" AND ").join(sqlBodies), variableBindings));
            case OR:
                return Optional.of(new JdbcSql(Joiner.on(" OR ").join(sqlBodies), variableBindings));
            case IN:
                return Optional.of(new JdbcSql(format("%s IN (%s)", sqlBodies.get(0), Joiner.on(" , ").join(sqlBodies.subList(1, sqlBodies.size()))), variableBindings));
        }
        return Optional.empty();
    }

    private static boolean isAcceptedType(Type type)
    {
        Type validType = requireNonNull(type, "type is null");
        return validType.equals(BigintType.BIGINT) ||
                validType.equals(TinyintType.TINYINT) ||
                validType.equals(SmallintType.SMALLINT) ||
                validType.equals(IntegerType.INTEGER) ||
                validType.equals(DoubleType.DOUBLE) ||
                validType.equals(RealType.REAL) ||
                validType.equals(BooleanType.BOOLEAN) ||
                validType.equals(DateType.DATE) ||
                validType.equals(TimeType.TIME) ||
                validType.equals(TimeWithTimeZoneType.TIME_WITH_TIME_ZONE) ||
                validType.equals(TimestampType.TIMESTAMP) ||
                validType.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE) ||
                validType instanceof VarcharType ||
                validType instanceof CharType;
    }
}

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
package com.facebook.presto.plugin.jdbc.optimization.function;

import com.facebook.presto.plugin.jdbc.optimization.JdbcSql;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.translator.TranslatedExpression;
import com.facebook.presto.spi.type.StandardTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class OperatorTranslators
{
    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGINT)
    public static JdbcSql add(@SqlType(StandardTypes.BIGINT) TranslatedExpression<JdbcSql> left, @SqlType(StandardTypes.BIGINT) TranslatedExpression<JdbcSql> right)
    {
        return new JdbcSql(infixOperation("+", left, right), forwardBindVariables(left, right));
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.BIGINT)
    public static JdbcSql subtract(@SqlType(StandardTypes.BIGINT) TranslatedExpression<JdbcSql> left, @SqlType(StandardTypes.BIGINT) TranslatedExpression<JdbcSql> right)
    {
        return new JdbcSql(infixOperation("-", left, right), forwardBindVariables(left, right));
    }

    @ScalarFunction("not")
    @SqlType(StandardTypes.BOOLEAN)
    public static JdbcSql not(@SqlType(StandardTypes.BOOLEAN) TranslatedExpression<JdbcSql> expression)
    {
        return new JdbcSql("NOT (" + expression.getTranslated().get().getSql() + ")", expression.getTranslated().get().getBindValues());
    }

    private static String infixOperation(String operator, TranslatedExpression<JdbcSql> left, TranslatedExpression<JdbcSql> right)
    {
        return left.getTranslated().get().getSql() + " " + operator + " " + right.getTranslated().get().getSql();
    }

    private static List<ConstantExpression> forwardBindVariables(TranslatedExpression<JdbcSql>... jdbcSqls)
    {
        return Arrays.stream(jdbcSqls).map(TranslatedExpression::getTranslated)
                .map(Optional::get)
                .map(JdbcSql::getBindValues)
                .flatMap(List::stream)
                .collect(toImmutableList());
    }
}

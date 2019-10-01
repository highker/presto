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

import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider.TargetExpression;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.function.OperatorType.ADD;

public final class BigintOperators
{
    private BigintOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.BIGINT)
    public static TargetExpression<String> add(@SqlType(StandardTypes.BIGINT) TargetExpression<String> left, @SqlType(StandardTypes.BIGINT) TargetExpression<String> right)
    {
        return new TargetExpression<>(
                ImmutableList.of(left, right),
                Optional.of(left.getTranslatedExpression().get() + " + " + right.getTranslatedExpression().get()));
    }
}

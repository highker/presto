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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.SortExpressionContext;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static com.facebook.presto.sql.planner.SortExpressionExtractor.extractSortExpression;

public final class JoinNodeUtil
{
    private JoinNodeUtil() {}

    public static Optional<SortExpressionContext> getSortExpressionContext(JoinNode node)
    {
        return node.getFilter()
                .flatMap(filter -> extractSortExpression(ImmutableSet.copyOf(node.getRight().getOutputSymbols()), filter));
    }
}

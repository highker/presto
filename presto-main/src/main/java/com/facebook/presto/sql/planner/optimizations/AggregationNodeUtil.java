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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.tree.FunctionCall;

import java.util.Optional;

public final class AggregationNodeUtil
{
    private AggregationNodeUtil() {}

    public static boolean isDecomposable(AggregationNode node, FunctionRegistry functionRegistry)
    {
        boolean hasOrderBy = node.getAggregations().values().stream()
                .map(AggregationNode.Aggregation::getCall)
                .map(FunctionCall::getOrderBy)
                .anyMatch(Optional::isPresent);

        boolean hasDistinct = node.getAggregations().values().stream()
                .map(AggregationNode.Aggregation::getCall)
                .anyMatch(FunctionCall::isDistinct);

        boolean decomposableFunctions = node.getAggregations().values().stream()
                .map(AggregationNode.Aggregation::getSignature)
                .map(functionRegistry::getAggregateFunctionImplementation)
                .allMatch(InternalAggregationFunction::isDecomposable);

        return !hasOrderBy && !hasDistinct && decomposableFunctions;
    }

    public static boolean hasSingleNodeExecutionPreference(AggregationNode node, FunctionRegistry functionRegistry)
    {
        // There are two kinds of aggregations the have single node execution preference:
        //
        // 1. aggregations with only empty grouping sets like
        //
        // SELECT count(*) FROM lineitem;
        //
        // there is no need for distributed aggregation. Single node FINAL aggregation will suffice,
        // since all input have to be aggregated into one line output.
        //
        // 2. aggregations that must produce default output and are not decomposable, we can not distribute them.
        return (node.hasEmptyGroupingSet() && !node.hasNonEmptyGroupingSet()) || (node.hasDefaultOutput() && !isDecomposable(node, functionRegistry));
    }
}

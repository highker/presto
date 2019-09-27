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

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

public class PinotPushdownUtils
{
    private PinotPushdownUtils() {}

    public enum ExprType
    {
        GROUP_BY,
        AGGREGATE,
    }

    /**
     * Group by field description
     */
    public static class GroupByColumnNode
            extends AggregationColumnNode
    {
        private final VariableReferenceExpression inputColumn;

        public GroupByColumnNode(VariableReferenceExpression inputColumn, VariableReferenceExpression output)
        {
            super(ExprType.GROUP_BY, output);
            this.inputColumn = inputColumn;
        }

        public VariableReferenceExpression getInputColumn()
        {
            return inputColumn;
        }

        @Override
        public String toString()
        {
            return inputColumn.toString();
        }
    }

    /**
     * Agg function description.
     */
    public static class AggregationFunctionColumnNode
            extends AggregationColumnNode
    {
        private final CallExpression callExpression;

        public AggregationFunctionColumnNode(VariableReferenceExpression output, CallExpression callExpression)
        {
            super(ExprType.AGGREGATE, output);
            this.callExpression = callExpression;
        }

        public CallExpression getCallExpression()
        {
            return callExpression;
        }

        @Override
        public String toString()
        {
            return callExpression.toString();
        }
    }

    public static void checkSupported(boolean condition, String errorMessage, Object... errorMessageArgs)
    {
        if (!condition) {
            throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), String.format(errorMessage, errorMessageArgs));
        }
    }

    public abstract static class AggregationColumnNode
    {
        private final ExprType exprType;
        private final VariableReferenceExpression outputColumn;

        public AggregationColumnNode(ExprType exprType, VariableReferenceExpression outputColumn)
        {
            this.exprType = exprType;
            this.outputColumn = outputColumn;
        }

        public VariableReferenceExpression getOutputColumn()
        {
            return outputColumn;
        }

        public ExprType getExprType()
        {
            return exprType;
        }
    }

    public static Optional<List<AggregationColumnNode>> computeAggregationNodes(AggregationNode aggregationNode)
    {
        int groupByKeyIndex = 0;
        ImmutableList.Builder<AggregationColumnNode> nodeBuilder = ImmutableList.builder();
        for (VariableReferenceExpression outputColumn : aggregationNode.getOutputVariables()) {
            AggregationNode.Aggregation agg = aggregationNode.getAggregations().get(outputColumn);

            if (agg != null) {
                CallExpression aggFunction = agg.getCall();
                if (!agg.getFilter().isPresent()
                        && !agg.isDistinct()
                        && !agg.getOrderBy().isPresent()
                        && !agg.getMask().isPresent()) {
                    nodeBuilder.add(new AggregationFunctionColumnNode(outputColumn, aggFunction));
                }
                else {
                    return Optional.empty();
                }
            }
            else {
                // group by output
                VariableReferenceExpression inputColumn = aggregationNode.getGroupingKeys().get(groupByKeyIndex);
                nodeBuilder.add(new GroupByColumnNode(inputColumn, outputColumn));
                groupByKeyIndex++;
            }
        }
        return Optional.of(nodeBuilder.build());
    }

    public static LinkedHashMap<VariableReferenceExpression, SortOrder> getOrderingScheme(TopNNode topNNode)
    {
        LinkedHashMap<VariableReferenceExpression, SortOrder> orderingScheme = new LinkedHashMap<>();
        topNNode.getOrderingScheme().getOrderByVariables().forEach(v -> orderingScheme.put(v, topNNode.getOrderingScheme().getOrdering(v)));
        return orderingScheme;
    }
}

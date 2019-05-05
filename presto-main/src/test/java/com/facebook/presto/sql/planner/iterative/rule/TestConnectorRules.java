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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.ConnectorRule;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableHandle;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.PlanOptimizers.DelegateRule;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.iterative.rule.test.RuleTester.CONNECTOR_ID;

public class TestConnectorRules
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new DelegateRule(new FilterPushdown()))
                .on(p -> p.filter(expression("b > 44"),
                        p.tableScan(
                                new TableHandle(
                                        CONNECTOR_ID.getCatalogName(),
                                        new TpchTableHandle("nation", 1.0),
                                        TestingTransactionHandle.create(),
                                        Optional.empty()),
                                ImmutableList.of(),
                                ImmutableMap.of())))
                .doesNotFire();
    }

    public class FilterPushdown
            implements ConnectorRule<FilterNode>
    {
        @Override
        public Class<FilterNode> getNodeType()
        {
            return FilterNode.class;
        }

        @Override
        public Result apply(PlanNode planNode)
        {
            FilterNode filterNode = (FilterNode) planNode;
            if (!(filterNode.getSource() instanceof TableScanNode)) {
                return Result.empty();
            }
            TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();

            if (!tableScanNode.getTable().getCatalog().equals("local")) {
                return Result.empty();
            }

            TableHandle tableHandle = tableScanNode.getTable();

            TableScanNode newTableScanNode = new TableScanNode(
                    tableScanNode.getId(),
                    new TableHandle(
                            tableHandle.getCatalog(),
                            tableHandle.getConnectorHandle(),
                            tableHandle.getTransaction(),
                            Optional.empty()),
                    tableScanNode.getOutputSymbols(),
                    tableScanNode.getAssignments());


            return Result.ofPlanNode(new FilterNode(
                    filterNode.getId(),
                    newTableScanNode,
                    filterNode.getPredicate()));
        }
    }
}

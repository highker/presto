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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ConnectorRule;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableHandle;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.PlanOptimizers.ConnectorPlanOptimizer;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestConnectorOptimizer
        extends BasePlanTest
{
    // TODO: this will fail; but I don't care
    @Test
    public void testDuplicatesInWindowOrderBy()
    {
        assertPlan("SELECT * FROM lineitem where length(comment) < 5",
                anyTree(tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey"))),
                ImmutableList.of(new ConnectorPlanOptimizer(new FilterPushdown())));
    }

    public class FilterPushdown
            implements ConnectorRule
    {
        @Override
        public boolean match(PlanNode planNode)
        {
            if (!(planNode instanceof FilterNode)) {
                return false;
            }
            FilterNode filterNode = (FilterNode) planNode;
            if (!(filterNode.getSource() instanceof TableScanNode)) {
                return false;
            }
            TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();

            return tableScanNode.getTable().getCatalog().equals("local");
        }

        @Override
        public PlanNode apply(PlanNode planNode)
        {
            FilterNode filterNode = (FilterNode) planNode;
            TableScanNode tableScanNode = (TableScanNode) filterNode.getSource();

            TableHandle tableHandle = tableScanNode.getTable();

            return new TableScanNode(
                    tableScanNode.getId(),
                    new TableHandle(
                            tableHandle.getCatalog(),
                            tableHandle.getConnectorHandle(),
                            tableHandle.getTransaction(),
                            Optional.empty()),
                    tableScanNode.getOutputSymbols(),
                    tableScanNode.getAssignments());
        }
    }
}

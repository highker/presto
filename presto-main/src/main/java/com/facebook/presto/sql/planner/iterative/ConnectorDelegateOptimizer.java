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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorOptimizerProvider;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TypeProvider;
import com.facebook.presto.sql.planner.ExtendedSymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;

import static java.util.Objects.requireNonNull;

public class ConnectorDelegateOptimizer
        implements PlanOptimizer
{
    private final ConnectorId connectorId;
    private final ConnectorOptimizerProvider optimizerProvider;

    public ConnectorDelegateOptimizer(ConnectorId connectorId, ConnectorOptimizerProvider optimizerProvider)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.optimizerProvider = requireNonNull(optimizerProvider, "optimizerProvider is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, ExtendedSymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);
        ConnectorPlanOptimizer optimizer = optimizerProvider.createPlanOptimizer(connectorSession);

        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");
        return optimizer.optimize(plan, connectorSession, types, symbolAllocator, idAllocator, warningCollector);
    }
}

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

import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.Symbol;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;

public final class ExchangeNodeUtil
{
    private ExchangeNodeUtil() {}

    public static ExchangeNode partitionedExchange(PlanNodeId id, ExchangeNode.Scope scope, PlanNode child, List<Symbol> partitioningColumns, Optional<Symbol> hashColumns)
    {
        return partitionedExchange(id, scope, child, partitioningColumns, hashColumns, false);
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, ExchangeNode.Scope scope, PlanNode child, List<Symbol> partitioningColumns, Optional<Symbol> hashColumns, boolean replicateNullsAndAny)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, partitioningColumns),
                        child.getOutputSymbols(),
                        hashColumns,
                        replicateNullsAndAny,
                        Optional.empty()));
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, ExchangeNode.Scope scope, PlanNode child, PartitioningScheme partitioningScheme)
    {
        if (partitioningScheme.getPartitioning().getHandle().isSingleNode()) {
            return gatheringExchange(id, scope, child);
        }
        return new ExchangeNode(
                id,
                ExchangeNode.Type.REPARTITION,
                scope,
                partitioningScheme,
                ImmutableList.of(child),
                ImmutableList.of(partitioningScheme.getOutputLayout()).asList(),
                Optional.empty());
    }

    public static ExchangeNode replicatedExchange(PlanNodeId id, ExchangeNode.Scope scope, PlanNode child)
    {
        return new ExchangeNode(
                id,
                ExchangeNode.Type.REPLICATE,
                scope,
                new PartitioningScheme(Partitioning.create(FIXED_BROADCAST_DISTRIBUTION, ImmutableList.of()), child.getOutputSymbols()),
                ImmutableList.of(child),
                ImmutableList.of(child.getOutputSymbols()),
                Optional.empty());
    }

    public static ExchangeNode gatheringExchange(PlanNodeId id, ExchangeNode.Scope scope, PlanNode child)
    {
        return new ExchangeNode(
                id,
                ExchangeNode.Type.GATHER,
                scope,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), child.getOutputSymbols()),
                ImmutableList.of(child),
                ImmutableList.of(child.getOutputSymbols()),
                Optional.empty());
    }

    public static ExchangeNode roundRobinExchange(PlanNodeId id, ExchangeNode.Scope scope, PlanNode child)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), child.getOutputSymbols()));
    }

    public static ExchangeNode mergingExchange(PlanNodeId id, ExchangeNode.Scope scope, PlanNode child, OrderingScheme orderingScheme)
    {
        PartitioningHandle partitioningHandle = scope == LOCAL ? FIXED_PASSTHROUGH_DISTRIBUTION : SINGLE_DISTRIBUTION;
        return new ExchangeNode(
                id,
                ExchangeNode.Type.GATHER,
                scope,
                new PartitioningScheme(Partitioning.create(partitioningHandle, ImmutableList.of()), child.getOutputSymbols()),
                ImmutableList.of(child),
                ImmutableList.of(child.getOutputSymbols()),
                Optional.of(orderingScheme));
    }
}

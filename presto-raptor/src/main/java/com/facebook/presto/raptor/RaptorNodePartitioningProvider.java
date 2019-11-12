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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorBucketNodeMap;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.function.ToIntFunction;

import static com.facebook.presto.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RaptorNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeSupplier nodeSupplier;

    @Inject
    public RaptorNodePartitioningProvider(NodeSupplier nodeSupplier)
    {
        this.nodeSupplier = requireNonNull(nodeSupplier, "nodeSupplier is null");
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning)
    {
        RaptorPartitioningHandle handle = (RaptorPartitioningHandle) partitioning;

        List<Node> nodes = nodeSupplier.getWorkerNodes().stream()
                .sorted(Comparator.comparing(Node::getNodeIdentifier))
                .collect(toImmutableList());

        ImmutableList.Builder<Node> bucketToNode = ImmutableList.builder();
        for (int i = 0; i < handle.getBucketToNode().size(); i++) {
            bucketToNode.add(nodes.get(i % nodes.size()));
        }
        return createBucketNodeMap(bucketToNode.build());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning)
    {
        return value -> ((RaptorSplit) value).getBucketNumber().getAsInt();
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning, List<Type> partitionChannelTypes, int bucketCount)
    {
        return new RaptorBucketFunction(bucketCount, partitionChannelTypes);
    }
}

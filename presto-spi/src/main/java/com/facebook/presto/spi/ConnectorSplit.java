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
package com.facebook.presto.spi;

import com.facebook.presto.spi.schedule.NodeSelectionStrategy;

import java.util.List;

public interface ConnectorSplit
{
    /**
     * Indicate the node affinity of a Split
     * 1. HARD_AFFINITY: Split is NOT remotely accessible and has to be on specific nodes
     * 2. SOFT_AFFINITY: Connector level cache
     * 3. NO_PREFERENCE: Split is remotely accessible and can be on any nodes
     */
    NodeSelectionStrategy getNodeSelectionStrategy();

    List<HostAddress> getAddresses();

    Object getInfo();
}

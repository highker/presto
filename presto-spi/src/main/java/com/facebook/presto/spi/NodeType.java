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

/**
 * COORDINATOR is a single process containing both DISPATCHER and QUERY_COORDINATOR
 */
public enum NodeType
{
    DISPATCHER, COORDINATOR, WORKER, QUERY_COORDINATOR;

    public static boolean isCoordinator(NodeType nodeType)
    {
        return nodeType == COORDINATOR || nodeType == QUERY_COORDINATOR;
    }

    /**
     * Even COORDINATOR is a superset of dispatcher,
     * this method should only return server that is doing pure query queueing
     */
    public static boolean isDispatcher(NodeType nodeType)
    {
        return nodeType == DISPATCHER;
    }

    public static boolean isWorker(NodeType nodeType)
    {
        return nodeType == WORKER;
    }
}

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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveSessionProperties.getNodeSelectionStrategy;
import static org.testng.Assert.assertEquals;

public class TestHiveSessionProperties
{
    @Test
    public void testEmptyNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig(),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig()).getSessionProperties());
        assertEquals(getNodeSelectionStrategy(connectorSession), NodeSelectionStrategy.NO_PREFERENCE);
    }

    @Test
    public void testEmptyConfigNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setNodeSelectionStrategy(""),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig()).getSessionProperties());
        assertEquals(getNodeSelectionStrategy(connectorSession), NodeSelectionStrategy.NO_PREFERENCE);
    }

    @Test
    public void testNodeSelectionStrategyConfig()
    {
        ConnectorSession connectorSession = new TestingConnectorSession(
                new HiveSessionProperties(
                        new HiveClientConfig().setNodeSelectionStrategy(NodeSelectionStrategy.HARD_AFFINITY.name()),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig()).getSessionProperties());
        assertEquals(getNodeSelectionStrategy(connectorSession), NodeSelectionStrategy.HARD_AFFINITY);
    }
}

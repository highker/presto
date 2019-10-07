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
package com.facebook.presto.hive.rule;

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class HivePlanOptimizerProvider
        implements ConnectorPlanOptimizerProvider
{
    @Override
    public Set<ConnectorPlanOptimizer> getConnectorPlanOptimizers(Context context)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<Class<?>> getFunctionTranslators()
    {
        return ImmutableSet.of();
    }
}

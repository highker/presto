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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.translator.FunctionTranslator;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.sql.planner.optimizations.connector.TranslatorAnnotationParser.parseFunctionTranslations;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.transformValues;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class ConnectorPlanOptimizerManager
{
    private final Map<ConnectorId, ConnectorPlanOptimizerProvider> planOptimizerProviders = new ConcurrentHashMap<>();

    @Inject
    public ConnectorPlanOptimizerManager() {}

    public void addPlanOptimizerProvider(ConnectorId connectorId, ConnectorPlanOptimizerProvider planOptimizerProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(planOptimizerProvider, "planOptimizerProvider is null");
        checkArgument(planOptimizerProviders.putIfAbsent(connectorId, planOptimizerProvider) == null,
                "ConnectorPlanOptimizerProvider for connector '%s' is already registered", connectorId);
    }

    public Map<ConnectorId, Set<ConnectorPlanOptimizer>> getOptimizers()
    {
        return ImmutableMap.copyOf(transformValues(planOptimizerProviders, provider -> provider.getConnectorPlanOptimizers(new ConnectorPlanOptimizerProvider.Context()
        {
            @Override
            public <T> Map<FunctionMetadata, FunctionTranslator<T>> getFunctionTranslatorMapping(Class<T> expressionType)
            {
                return getFunctionMapping(expressionType, provider.getFunctionTranslators());
            }
        })));
    }

    private <T> Map<FunctionMetadata, FunctionTranslator<T>> getFunctionMapping(Class<T> returnClazz, Set<Class<?>> translatorContainers)
    {
        return translatorContainers.stream()
                .map(containerClazz -> parseFunctionTranslations(returnClazz, containerClazz))
                .map(Map::entrySet)
                .flatMap(Set::stream)
                .collect(toMap(Map.Entry::getKey, entry -> (FunctionTranslator<T>) translatedArguments -> {
                    try {
                        return Optional.ofNullable((T) entry.getValue().invokeWithArguments(translatedArguments));
                    }
                    catch (Throwable t) {
                        return Optional.empty();
                    }
                }));
    }
}

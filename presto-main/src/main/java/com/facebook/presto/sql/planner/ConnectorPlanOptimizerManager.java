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
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider.TargetExpression;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider.TranslationContext;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.operator.scalar.annotations.FunctionTranslationFromAnnotationsParser.parseFunctionTranslations;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class ConnectorPlanOptimizerManager
{
    private final FunctionMetadataManager functionMetadataManager;
    private final Map<ConnectorId, ConnectorPlanOptimizerProvider<?>> planOptimizerProviders = new ConcurrentHashMap<>();

    // TODO: make this lazy like planOptimizerProviders
    private final Map<ConnectorId, Map<FunctionMetadata, MethodHandle>> functionTranslations = new ConcurrentHashMap<>();

    @Inject
    public ConnectorPlanOptimizerManager(FunctionMetadataManager functionMetadataManager)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
    }

    public void addPlanOptimizerProvider(ConnectorId connectorId, ConnectorPlanOptimizerProvider<?> planOptimizerProvider)
    {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(planOptimizerProvider, "planOptimizerProvider is null");
        checkArgument(planOptimizerProviders.putIfAbsent(connectorId, planOptimizerProvider) == null,
                "ConnectorPlanOptimizerProvider for connector '%s' is already registered", connectorId);

        ImmutableMap.Builder<FunctionMetadata, MethodHandle> translations = ImmutableMap.builder();
        planOptimizerProvider.getFunctionTranslation().forEach(translation -> translations.putAll(parseFunctionTranslations(translation)));

        functionTranslations.putIfAbsent(connectorId, translations.build());
    }

    public Map<ConnectorId, Set<ConnectorPlanOptimizer>> getOptimizers()
    {
        ImmutableMap.Builder<ConnectorId, Set<ConnectorPlanOptimizer>> optimizers = ImmutableMap.builder();

        for (Map.Entry<ConnectorId, ConnectorPlanOptimizerProvider<?>> provider : planOptimizerProviders.entrySet()) {
            ConnectorId connectorId = provider.getKey();
            ConnectorPlanOptimizerProvider<?> optimizer = provider.getValue();

            // TODO: resolve the wildcard issue
            TranslationContext context = new TranslationContext((handle, list) -> this.translateFunction(connectorId, (FunctionHandle) handle, (List<TargetExpression<?>>) list));
            optimizers.put(connectorId, optimizer.getConnectorPlanOptimizers(context));
        }

        return optimizers.build();
    }

    private TargetExpression<?> translateFunction(ConnectorId connectorId, FunctionHandle handle, List<TargetExpression<?>> arguments)
    {
        MethodHandle methodHandle = functionTranslations.get(connectorId).get(functionMetadataManager.getFunctionMetadata(handle));

        if (methodHandle == null) {
            return new TargetExpression(arguments, Optional.empty());
        }

        try {
            return (TargetExpression) methodHandle.invokeWithArguments(arguments);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(t);
        }
    }
}

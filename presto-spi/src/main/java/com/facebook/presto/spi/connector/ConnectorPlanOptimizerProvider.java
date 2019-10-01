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
package com.facebook.presto.spi.connector;

import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.function.FunctionHandle;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Collections.emptySet;

public interface ConnectorPlanOptimizerProvider<T>
{
    /**
     * @return a list of function mappings for registration.
     * The function translation will be used by {@link #getConnectorPlanOptimizers(TranslationContext)}
     */
    default Set<Class<?>> getFunctionTranslation()
    {
        return emptySet();
    }

    /**
     * @param functionTranslator that maps CallExpression to TargetExpression.
     * @return a set of optimizer rules.
     */
    Set<ConnectorPlanOptimizer> getConnectorPlanOptimizers(TranslationContext<T> functionTranslator);

    final class TargetExpression<T>
    {
        private final List<TargetExpression> parameters;
        private final Optional<T> translatedExpression;

        public TargetExpression(List<TargetExpression> parameters, Optional<T> translatedExpression)
        {
            this.parameters = parameters;
            this.translatedExpression = translatedExpression;
        }

        public List<TargetExpression> getParameters()
        {
            return parameters;
        }

        public Optional<T> getTranslatedExpression()
        {
            return translatedExpression;
        }
    }

    final class TranslationContext<T>
    {
        // TODO: add other translators
        BiFunction<FunctionHandle, List<TargetExpression<T>>, TargetExpression<T>> functionTranslator;

        public TranslationContext(BiFunction<FunctionHandle, List<TargetExpression<T>>, TargetExpression<T>> functionTranslator)
        {
            this.functionTranslator = functionTranslator;
        }

        public BiFunction<FunctionHandle, List<TargetExpression<T>>, TargetExpression<T>> getFunctionTranslator()
        {
            return functionTranslator;
        }
    }
}

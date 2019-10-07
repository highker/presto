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
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.translator.TranslatedExpression;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ConnectorPlanOptimizerProvider
{
    Set<ConnectorPlanOptimizer> getConnectorPlanOptimizers(Context context);

    default Set<Class<?>> registerFunctionMapping()
    {
        return Collections.emptySet();
    }

    class Context
    {
        public Context(FunctionTranslator functionTranslator)
        {
            this.functionTranslator = functionTranslator;
        }

        FunctionTranslator functionTranslator;

        FunctionTranslator getGetFunctionTranslator()
        {
            return functionTranslator;
        }
    }

    /**
     * 1:1 function mapping
     * @param <T>
     */
    class FunctionTranslator<T>
    {
        private Map<FunctionMetadata, FunctionTranslator<T>> mapping;

        TranslatedExpression<T> translate(FunctionHandle handle, List<TranslatedExpression<T>> translatedArguments);
    }
}

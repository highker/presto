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
package com.facebook.presto.spi.relation.translator;

import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Optional;

public class RowExpressionTranslator<T, C>
{
    /**
     * translator for function that is recursive. Will be overidden if non-recursive version returns translated values.
     */
    public Optional<T> translateFunction(FunctionHandle functionHandle, List<TranslatedExpression<T>> translatedExpressions, C context)
    {
        return Optional.empty();
    }

    /**
     * translator for special form that is recursive. Will be overidden if non-recursive version returns translated values.
     */
    public Optional<T> translateSpecialForm(SpecialFormExpression.Form form, List<TranslatedExpression<T>> translatedExpressions, C context)
    {
        return Optional.empty();
    }

    public Optional<T> translateConstant(ConstantExpression literal, C context)
    {
        return Optional.empty();
    }

    public Optional<T> translateVariable(VariableReferenceExpression reference, C context)
    {
        return Optional.empty();
    }

    /**
     * default translator for function that is non-recursive.
     */
    public Optional<TranslatedExpression<T>> translateFunction(CallExpression call, C context, RowExpressionTreeTranslator<T, C> rowExpressionTreeTranslator)
    {
        return translateExpression(call, context, rowExpressionTreeTranslator);
    }

    /**
     * default translator for special form that is non-recursive.
     */
    public Optional<TranslatedExpression<T>> translateSpecialForm(SpecialFormExpression specialForm, C context, RowExpressionTreeTranslator<T, C> rowExpressionTreeTranslator)
    {
        return translateExpression(specialForm, context, rowExpressionTreeTranslator);
    }

    /**
     * default translator for expression that is non-recursive.
     */
    public Optional<TranslatedExpression<T>> translateExpression(RowExpression expression, C context, RowExpressionTreeTranslator<T, C> rowExpressionTreeTranslator)
    {
        return Optional.empty();
    }
}

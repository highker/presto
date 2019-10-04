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

import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

public class RowExpressionTreeTranslator<T, C>
{
    private final RowExpressionTranslator<T, C> rowExpressionTranslator;
    private final RowExpressionVisitor<TranslatedExpression<T>, C> visitor;

    private RowExpressionTreeTranslator(RowExpressionTranslator<T, C> rowExpressionTranslator)
    {
        this.rowExpressionTranslator = rowExpressionTranslator;
        this.visitor = new TranslatingVisitor();
    }

    public static <T, C> TranslatedExpression<T> translateWith(RowExpression expression, RowExpressionTranslator<T, C> translator, C context)
    {
        return expression.accept(new RowExpressionTreeTranslator<>(translator).visitor, context);
    }

    private class TranslatingVisitor
            implements RowExpressionVisitor<TranslatedExpression<T>, C>
    {
        @Override
        public TranslatedExpression<T> visitCall(CallExpression call, C context)
        {
            Optional<TranslatedExpression<T>> defaultTranslated = rowExpressionTranslator.translateFunction(call, context, RowExpressionTreeTranslator.this);
            if (defaultTranslated.isPresent()) {
                return defaultTranslated.get();
            }
            List<TranslatedExpression<T>> translatedArguments = call.getArguments().stream()
                    .map(expression -> expression.accept(this, context))
                    .collect(Collectors.toList());
            Optional<T> result = rowExpressionTranslator.translateFunction(call.getFunctionHandle(), translatedArguments, context);
            return new TranslatedExpression<>(result, call, translatedArguments);
        }

        @Override
        public TranslatedExpression<T> visitInputReference(InputReferenceExpression reference, C context)
        {
            throw new UnsupportedOperationException("Cannot translate RowExpression that contains inputReferenceExpression");
        }

        @Override
        public TranslatedExpression<T> visitConstant(ConstantExpression literal, C context)
        {
            Optional<TranslatedExpression<T>> defaultTranslated = rowExpressionTranslator.translateExpression(literal, context, RowExpressionTreeTranslator.this);
            if (defaultTranslated.isPresent()) {
                return defaultTranslated.get();
            }
            return new TranslatedExpression<>(rowExpressionTranslator.translateConstant(literal, context), literal, emptyList());
        }

        @Override
        public TranslatedExpression<T> visitLambda(LambdaDefinitionExpression lambda, C context)
        {
            return TranslatedExpression.untranslated(lambda);
        }

        @Override
        public TranslatedExpression<T> visitVariableReference(VariableReferenceExpression reference, C context)
        {
            Optional<TranslatedExpression<T>> defaultTranslated = rowExpressionTranslator.translateExpression(reference, context, RowExpressionTreeTranslator.this);
            if (defaultTranslated.isPresent()) {
                return defaultTranslated.get();
            }
            return new TranslatedExpression<>(rowExpressionTranslator.translateVariable(reference, context), reference, emptyList());
        }

        @Override
        public TranslatedExpression<T> visitSpecialForm(SpecialFormExpression specialForm, C context)
        {
            Optional<TranslatedExpression<T>> defaultTranslated = rowExpressionTranslator.translateSpecialForm(specialForm, context, RowExpressionTreeTranslator.this);
            if (defaultTranslated.isPresent()) {
                return defaultTranslated.get();
            }
            List<TranslatedExpression<T>> translatedArguments = specialForm.getArguments().stream()
                    .map(expression -> expression.accept(this, context))
                    .collect(Collectors.toList());
            Optional<T> result = rowExpressionTranslator.translateSpecialForm(specialForm.getForm(), translatedArguments, context);
            return new TranslatedExpression<>(result, specialForm, translatedArguments);
        }
    }
}

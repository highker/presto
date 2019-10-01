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
package com.facebook.presto.spi.relation;

import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider.TargetExpression;
import com.facebook.presto.spi.connector.ConnectorPlanOptimizerProvider.TranslationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;

public final class RowExpressionTreeTranslator<C>
{
    private final RowExpressionTranslator<C> translator;
    private final RowExpressionVisitor<TargetExpression<C>, TranslationContext<C>> visitor;

    public static <C> TargetExpression<C> defaultTranslate(RowExpression node, TranslationContext<C> context)
    {
        return new RowExpressionTreeTranslator<C>(new RowExpressionTranslator<>()).translate(node, context);
    }

    public RowExpressionTreeTranslator(RowExpressionTranslator<C> translator)
    {
        this.translator = translator;
        this.visitor = new TranslatingVisitor();
    }

    private List<TargetExpression<C>> translate(List<RowExpression> items, TranslationContext<C> context)
    {
        List<TargetExpression<C>> translateExpressions = new ArrayList<>();
        for (RowExpression expression : items) {
            translateExpressions.add(translate(expression, context));
        }
        return Collections.unmodifiableList(translateExpressions);
    }

    public TargetExpression<C> translate(RowExpression node, TranslationContext<C> context)
    {
        return node.accept(visitor, context);
    }

    private class TranslatingVisitor
            implements RowExpressionVisitor<TargetExpression<C>, TranslationContext<C>>
    {
        @Override
        public TargetExpression<C> visitInputReference(InputReferenceExpression input, TranslationContext<C> context)
        {
            return translator.translateInputReference(input, emptyList(), context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TargetExpression<C> visitCall(CallExpression call, TranslationContext<C> context)
        {
            List<TargetExpression<C>> arguments = translate(call.getArguments(), context);
            return translator.translateCall(call, arguments, context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TargetExpression<C> visitConstant(ConstantExpression literal, TranslationContext<C> context)
        {
            return translator.translateConstant(literal, emptyList(), context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TargetExpression<C> visitLambda(LambdaDefinitionExpression lambda, TranslationContext<C> context)
        {
            return translator.translateLambda(lambda, emptyList(), context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TargetExpression<C> visitVariableReference(VariableReferenceExpression variable, TranslationContext<C> context)
        {
            return translator.translateVariableReference(variable, emptyList(), context, RowExpressionTreeTranslator.this);
        }

        @Override
        public TargetExpression<C> visitSpecialForm(SpecialFormExpression specialForm, TranslationContext<C> context)
        {
            List<TargetExpression<C>> arguments = translate(specialForm.getArguments(), context);
            return translator.translateSpecialForm(specialForm, arguments, context, RowExpressionTreeTranslator.this);
        }
    }
}

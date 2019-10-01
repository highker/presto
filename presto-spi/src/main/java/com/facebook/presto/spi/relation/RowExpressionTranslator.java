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
import com.facebook.presto.spi.function.FunctionHandle;

import java.util.List;
import java.util.function.BiFunction;

/**
 * Connector can override this default translator to specialize translation
 */
public class RowExpressionTranslator<C>
{
    public TargetExpression<C> translateRowExpression(RowExpression node, List<TargetExpression<C>> arguments, TranslationContext<C> context, RowExpressionTreeTranslator<C> treeRewriter)
    {
        return null;
    }

    public TargetExpression<C> translateInputReference(InputReferenceExpression node, List<TargetExpression<C>> arguments, TranslationContext<C> context, RowExpressionTreeTranslator<C> treeRewriter)
    {
        return translateRowExpression(node, arguments, context, treeRewriter);
    }

    public TargetExpression<C> translateCall(CallExpression node, List<TargetExpression<C>> arguments, TranslationContext<C> context, RowExpressionTreeTranslator<C> treeRewriter)
    {
        BiFunction<FunctionHandle, List<TargetExpression<C>>, TargetExpression<C>> functionTranslator = context.getFunctionTranslator();
        return functionTranslator.apply(node.getFunctionHandle(), arguments);
    }

    public TargetExpression<C> translateConstant(ConstantExpression node, List<TargetExpression<C>> arguments, TranslationContext<C> context, RowExpressionTreeTranslator<C> treeRewriter)
    {
        return translateRowExpression(node, arguments, context, treeRewriter);
    }

    public TargetExpression<C> translateLambda(LambdaDefinitionExpression node, List<TargetExpression<C>> arguments, TranslationContext<C> context, RowExpressionTreeTranslator<C> treeRewriter)
    {
        return translateRowExpression(node, arguments, context, treeRewriter);
    }

    public TargetExpression<C> translateVariableReference(VariableReferenceExpression node, List<TargetExpression<C>> arguments, TranslationContext<C> context, RowExpressionTreeTranslator<C> treeRewriter)
    {
        return translateRowExpression(node, arguments, context, treeRewriter);
    }

    public TargetExpression<C> translateSpecialForm(SpecialFormExpression node, List<TargetExpression<C>> arguments, TranslationContext<C> context, RowExpressionTreeTranslator<C> treeRewriter)
    {
        return translateRowExpression(node, arguments, context, treeRewriter);
    }
}

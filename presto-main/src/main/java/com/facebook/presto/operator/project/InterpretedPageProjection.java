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
package com.facebook.presto.operator.project;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputParameterRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypesFromInput;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class InterpretedPageProjection
        implements PageProjection
{
    private final PageProjection compiledProjection;
    private final ExpressionInterpreter evaluator;
    private final InputChannels inputChannels;
    private final boolean deterministic;
    private BlockBuilder blockBuilder;

    public InterpretedPageProjection(
            PageProjection compiledProjection,
            Expression expression,
            Map<Symbol, Type> symbolTypes,
            Map<Symbol, Integer> symbolToInputMappings,
            Metadata metadata,
            SqlParser sqlParser,
            Session session)
    {
        this.compiledProjection = compiledProjection;
        SymbolToInputParameterRewriter rewriter = new SymbolToInputParameterRewriter(symbolTypes, symbolToInputMappings);
        Expression rewritten = rewriter.rewrite(expression);
        this.inputChannels = new InputChannels(rewriter.getInputChannels());
        this.deterministic = DeterminismEvaluator.isDeterministic(expression);

        // analyze rewritten expression so we can know the type of every expression in the tree
        List<Type> inputTypes = rewriter.getInputTypes();
        ImmutableMap.Builder<Integer, Type> parameterTypes = ImmutableMap.builder();
        for (int parameter = 0; parameter < inputTypes.size(); parameter++) {
            Type type = inputTypes.get(parameter);
            parameterTypes.put(parameter, type);
        }
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypesFromInput(session, metadata, sqlParser, parameterTypes.build(), rewritten, emptyList());
        this.evaluator = ExpressionInterpreter.expressionInterpreter(rewritten, metadata, session, expressionTypes);

        blockBuilder = evaluator.getType().createBlockBuilder(new BlockBuilderStatus(), 1);
    }

    @Override
    public Type getType()
    {
        return evaluator.getType();
    }

    @Override
    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        return new InterpretedPageProjectionWork(session, yieldSignal, page, selectedPositions);
    }

    private class InterpretedPageProjectionWork
            implements Work<Block>
    {
        private final DriverYieldSignal yieldSignal;
        private final Page page;
        private final SelectedPositions selectedPositions;
        private final ConnectorSession session;
        private final Method projection_object;
        private final Work<Block> work;

        private int nextIndexOrPosition;
        private Block result;

        public InterpretedPageProjectionWork(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
            this.page = requireNonNull(page, "page is null");
            this.selectedPositions = requireNonNull(selectedPositions, "selectedPositions is null");
            this.nextIndexOrPosition = selectedPositions.getOffset();
            this.session = session;
            try {
                this.work = compiledProjection.project(session, yieldSignal, page, selectedPositions);
                if (work instanceof InputPageProjection.CompletedWork) {
                    this.projection_object = null;
                }
                else {
                    this.projection_object = work.getClass().getDeclaredMethod("evaluate_value", ConnectorSession.class, Page.class, int.class);
                }
            }
            catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean process()
        {
            checkState(result == null, "result has been generated");
            int length = selectedPositions.getOffset() + selectedPositions.size();
            if (selectedPositions.isList()) {
                int[] positions = selectedPositions.getPositions();
                while (nextIndexOrPosition < length) {
                    try {
                        if (work instanceof InputPageProjection.CompletedWork) {
                            InputPageProjection.CompletedWork inputWork = (InputPageProjection.CompletedWork) work;
                            inputWork.getType().appendTo(inputWork.getResult(), positions[nextIndexOrPosition], blockBuilder);
                        }
                        else {
                            Object value = projection_object.invoke(work, session, page, positions[nextIndexOrPosition]);
                            writeNativeValue(evaluator.getType(), blockBuilder, value);
                        }
                    }
                    catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    nextIndexOrPosition++;
                    if (yieldSignal.isSet()) {
                        return false;
                    }
                }
            }
            else {
                while (nextIndexOrPosition < length) {
                    try {
                        if (work instanceof InputPageProjection.CompletedWork) {
                            InputPageProjection.CompletedWork inputWork = (InputPageProjection.CompletedWork) work;
                            inputWork.getType().appendTo(inputWork.getResult(), nextIndexOrPosition, blockBuilder);
                        }
                        else {
                            Object value = projection_object.invoke(work, session, page, nextIndexOrPosition);
                            writeNativeValue(evaluator.getType(), blockBuilder, value);
                        }
                    }
                    catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    nextIndexOrPosition++;
                    if (yieldSignal.isSet()) {
                        return false;
                    }
                }
            }

            result = blockBuilder.build();
            blockBuilder = blockBuilder.newBlockBuilderLike(new BlockBuilderStatus());
            return true;
        }

        @Override
        public Block getResult()
        {
            checkState(result != null, "result has not been generated");
            return result;
        }
    }
}

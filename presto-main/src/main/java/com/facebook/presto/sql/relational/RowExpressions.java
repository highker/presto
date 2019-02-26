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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.relational.Signatures.AND;
import static com.facebook.presto.sql.relational.Signatures.OR;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class RowExpressions
{
    public static final ConstantExpression TRUE = new ConstantExpression(true, BOOLEAN);
    public static final ConstantExpression FALSE = new ConstantExpression(false, BOOLEAN);

    private final DeterminismEvaluator determinismEvaluator;

    public RowExpressions(FunctionManager functionManager)
    {
        this.determinismEvaluator = new DeterminismEvaluator(requireNonNull(functionManager, "functionManager is null"));
    }

    public static List<RowExpression> extractConjuncts(RowExpression expression)
    {
        return extractPredicates(AND, expression);
    }

    public static List<RowExpression> extractDisjuncts(RowExpression expression)
    {
        return extractPredicates(OR, expression);
    }

    public static List<RowExpression> extractPredicates(RowExpression expression)
    {
        if (expression instanceof CallExpression) {
            return extractPredicates(((CallExpression) expression).getSignature().getName(), expression);
        }
        return ImmutableList.of(expression);
    }

    public static List<RowExpression> extractPredicates(String operator, RowExpression expression)
    {
        if (expression instanceof CallExpression && ((CallExpression) expression).getSignature().getName().equals(operator)) {
            CallExpression callExpression = (CallExpression) expression;
            verify(callExpression.getArguments().size() == 2, "AND expression requires exactly 2 operands");
            return ImmutableList.<RowExpression>builder()
                    .addAll(extractPredicates(operator, callExpression.getArguments().get(0)))
                    .addAll(extractPredicates(operator, callExpression.getArguments().get(1)))
                    .build();
        }

        return ImmutableList.of(expression);
    }

    public static RowExpression and(RowExpression... expressions)
    {
        return and(Arrays.asList(expressions));
    }

    public static RowExpression and(Collection<RowExpression> expressions)
    {
        return binaryExpression(AND, expressions);
    }

    public static RowExpression or(RowExpression... expressions)
    {
        return or(Arrays.asList(expressions));
    }

    public static RowExpression or(Collection<RowExpression> expressions)
    {
        return binaryExpression(OR, expressions);
    }

    public static RowExpression binaryExpression(String operator, Collection<RowExpression> expressions)
    {
        requireNonNull(operator, "operator is null");
        requireNonNull(expressions, "expressions is null");

        if (expressions.isEmpty()) {
            switch (operator) {
                case AND:
                    return TRUE;
                case OR:
                    return FALSE;
                default:
                    throw new IllegalArgumentException("Unsupported binary expression operator");
            }
        }

        // Build balanced tree for efficient recursive processing that
        // preserves the evaluation order of the input expressions.
        //
        // The tree is built bottom up by combining pairs of elements into
        // binary AND expressions.
        //
        // Example:
        //
        // Initial state:
        //  a b c d e
        //
        // First iteration:
        //
        //  /\    /\   e
        // a  b  c  d
        //
        // Second iteration:
        //
        //    / \    e
        //  /\   /\
        // a  b c  d
        //
        //
        // Last iteration:
        //
        //      / \
        //    / \  e
        //  /\   /\
        // a  b c  d

        Queue<RowExpression> queue = new ArrayDeque<>(expressions);
        while (queue.size() > 1) {
            Queue<RowExpression> buffer = new ArrayDeque<>();

            // combine pairs of elements
            while (queue.size() >= 2) {
                List<RowExpression> arguments = ImmutableList.of(queue.remove(), queue.remove());
                buffer.add(new CallExpression(buildSignature(operator, arguments), BOOLEAN, arguments));
            }

            // if there's and odd number of elements, just append the last one
            if (!queue.isEmpty()) {
                buffer.add(queue.remove());
            }

            // continue processing the pairs that were just built
            queue = buffer;
        }

        return queue.remove();
    }

    public RowExpression combinePredicates(String operator, RowExpression... expressions)
    {
        return combinePredicates(operator, Arrays.asList(expressions));
    }

    public RowExpression combinePredicates(String operator, Collection<RowExpression> expressions)
    {
        if (operator.equals(AND)) {
            return combineConjuncts(expressions);
        }

        return combineDisjuncts(expressions);
    }

    public RowExpression combineConjuncts(RowExpression... expressions)
    {
        return combineConjuncts(Arrays.asList(expressions));
    }

    public RowExpression combineConjuncts(Collection<RowExpression> expressions)
    {
        requireNonNull(expressions, "expressions is null");

        List<RowExpression> conjuncts = expressions.stream()
                .flatMap(e -> extractConjuncts(e).stream())
                .filter(e -> !e.equals(TRUE))
                .collect(toList());

        conjuncts = removeDuplicates(conjuncts);

        if (conjuncts.contains(FALSE)) {
            return FALSE;
        }

        return and(conjuncts);
    }

    public RowExpression combineDisjuncts(RowExpression... expressions)
    {
        return combineDisjuncts(Arrays.asList(expressions));
    }

    public RowExpression combineDisjuncts(Collection<RowExpression> expressions)
    {
        return combineDisjunctsWithDefault(expressions, FALSE);
    }

    public RowExpression combineDisjunctsWithDefault(Collection<RowExpression> expressions, RowExpression emptyDefault)
    {
        requireNonNull(expressions, "expressions is null");

        List<RowExpression> disjuncts = expressions.stream()
                .flatMap(e -> extractDisjuncts(e).stream())
                .filter(e -> !e.equals(FALSE))
                .collect(toList());

        disjuncts = removeDuplicates(disjuncts);

        if (disjuncts.contains(TRUE)) {
            return TRUE;
        }

        return disjuncts.isEmpty() ? emptyDefault : or(disjuncts);
    }

    private static Signature buildSignature(String operator, List<RowExpression> arguments)
    {
        return new Signature(operator, SCALAR, BOOLEAN.getTypeSignature(), arguments.stream().map(RowExpression::getType).map(Type::getTypeSignature).collect(toImmutableList()));
    }

    /**
     * Removes duplicate deterministic expressions. Preserves the relative order
     * of the expressions in the list.
     */
    private List<RowExpression> removeDuplicates(List<RowExpression> expressions)
    {
        Set<RowExpression> seen = new HashSet<>();

        ImmutableList.Builder<RowExpression> result = ImmutableList.builder();
        for (RowExpression expression : expressions) {
            if (!determinismEvaluator.isDeterministic(expression)) {
                result.add(expression);
            }
            else if (!seen.contains(expression)) {
                result.add(expression);
                seen.add(expression);
            }
        }

        return result.build();
    }
}

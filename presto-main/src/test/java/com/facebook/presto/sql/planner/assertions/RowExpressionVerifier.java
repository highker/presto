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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.relational.StandardFunctionResolution;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.function.OperatorType.ADD;
import static com.facebook.presto.spi.function.OperatorType.DIVIDE;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.MODULUS;
import static com.facebook.presto.spi.function.OperatorType.MULTIPLY;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.SWITCH;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.WHEN;
import static com.facebook.presto.sql.planner.RowExpressionInterpreter.rowExpressionInterpreter;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.AND;
import static com.facebook.presto.sql.tree.LogicalBinaryExpression.Operator.OR;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * RowExpression visitor which verifies if given expression (actual) is matching other RowExpression given as context (expected).
 */
final class RowExpressionVerifier
        extends AstVisitor<Boolean, RowExpression>
{
    // either use variable or input reference for symbol mapping
    private final SymbolAliases symbolAliases;
    private final Metadata metadata;
    private final Session session;
    private final StandardFunctionResolution functionResolution;

    RowExpressionVerifier(SymbolAliases symbolAliases, Metadata metadata, Session session)
    {
        this.symbolAliases = requireNonNull(symbolAliases, "symbolLayout is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.functionResolution = new StandardFunctionResolution(metadata.getFunctionManager());
    }

    @Override
    protected Boolean visitNode(Node node, RowExpression context)
    {
        throw new IllegalStateException(format("Node %s is not supported", node));
    }

    @Override
    protected Boolean visitTryExpression(TryExpression actual, RowExpression expected)
    {
        if (!(expected instanceof CallExpression) || !((CallExpression) expected).getFunctionHandle().getSignature().getName().equals("TRY")) {
            return false;
        }

        return process(actual.getInnerExpression(), ((CallExpression) expected).getArguments().get(0));
    }

    @Override
    protected Boolean visitCast(Cast actual, RowExpression expected)
    {
        // TODO: clean up cast path
        if (expected instanceof ConstantExpression && actual.getExpression() instanceof Literal && actual.getType().equals(expected.getType().toString())) {
            Literal literal = (Literal) actual.getExpression();
            if (literal instanceof StringLiteral) {
                Object value = LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) expected);
                String expectedString = value instanceof Slice ? ((Slice) value).toStringUtf8() : String.valueOf(value);
                return ((StringLiteral) literal).getValue().equals(expectedString);
            }
            return getValueFromLiteral(literal).equals(String.valueOf(LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) expected)));
        }
        if (expected instanceof VariableReferenceExpression && actual.getExpression() instanceof SymbolReference && actual.getType().equals(expected.getType().toString())) {
            return visitSymbolReference((SymbolReference) actual.getExpression(), expected);
        }
        if (!(expected instanceof CallExpression) || !functionResolution.isCastFunction(((CallExpression) expected).getFunctionHandle())) {
            return false;
        }

        if (!actual.getType().equals(expected.getType().toString())) {
            return false;
        }

        return process(actual.getExpression(), ((CallExpression) expected).getArguments().get(0));
    }

    @Override
    protected Boolean visitIsNullPredicate(IsNullPredicate actual, RowExpression expected)
    {
        if (!(expected instanceof SpecialFormExpression) || !((SpecialFormExpression) expected).getForm().equals(IS_NULL)) {
            return false;
        }

        return process(actual.getValue(), ((SpecialFormExpression) expected).getArguments().get(0));
    }

    @Override
    protected Boolean visitIsNotNullPredicate(IsNotNullPredicate actual, RowExpression expected)
    {
        if (!(expected instanceof CallExpression) || !functionResolution.notFunction().equals(((CallExpression) expected).getFunctionHandle())) {
            return false;
        }

        RowExpression argument = ((CallExpression) expected).getArguments().get(0);

        if (!(argument instanceof SpecialFormExpression) || !((SpecialFormExpression) argument).getForm().equals(IS_NULL)) {
            return false;
        }

        return process(actual.getValue(), ((SpecialFormExpression) argument).getArguments().get(0));
    }

    @Override
    protected Boolean visitInPredicate(InPredicate actual, RowExpression expected)
    {
        if (expected instanceof SpecialFormExpression && ((SpecialFormExpression) expected).getForm().equals(IN)) {
            List<RowExpression> arguments = ((SpecialFormExpression) expected).getArguments();
            if (actual.getValueList() instanceof InListExpression) {
                return process(actual.getValue(), arguments.get(0)) && process(((InListExpression) actual.getValueList()).getValues(), arguments.subList(1, arguments.size()));
            }
            else {
                /*
                 * If the expected value is a value list, but the actual is e.g. a SymbolReference,
                 * we need to unpack the value from the list so that when we hit visitSymbolReference, the
                 * expected.toString() call returns something that the symbolAliases actually contains.
                 * For example, InListExpression.toString returns "(onlyitem)" rather than "onlyitem".
                 *
                 * This is required because actual passes through the analyzer, planner, and possibly optimizers,
                 * one of which sometimes takes the liberty of unpacking the InListExpression.
                 *
                 * Since the expected value doesn't go through all of that, we have to deal with the case
                 * of the actual value being unpacked, but the expected value being an InListExpression.
                 */
                checkState(arguments.size() == 2, "Multiple expressions in expected value list %s, but actual value is not a list", arguments.subList(1, arguments.size()), actual.getValue());
                return process(actual.getValue(), arguments.get(0)) && process(actual.getValueList(), arguments.get(1));
            }
        }
        return false;
    }

    @Override
    protected Boolean visitComparisonExpression(ComparisonExpression actual, RowExpression expected)
    {
        if (expected instanceof CallExpression) {
            FunctionMetadata functionMetadata = metadata.getFunctionManager().getFunctionMetadata(((CallExpression) expected).getFunctionHandle());
            if (!functionMetadata.getOperatorType().isPresent() || !functionMetadata.getOperatorType().get().isComparisonOperator()) {
                return false;
            }
            OperatorType expectedOperatorType = functionMetadata.getOperatorType().get();
            OperatorType actualOperatorType = getOperatorType(actual.getOperator());
            if (actualOperatorType.equals(expectedOperatorType)) {
                return process(actual.getLeft(), ((CallExpression) expected).getArguments().get(0)) && process(actual.getRight(), ((CallExpression) expected).getArguments().get(1));
            }
        }
        return false;
    }

    private static OperatorType getOperatorType(ComparisonExpression.Operator operator)
    {
        OperatorType operatorType;
        switch (operator) {
            case EQUAL:
                operatorType = EQUAL;
                break;
            case NOT_EQUAL:
                operatorType = NOT_EQUAL;
                break;
            case LESS_THAN:
                operatorType = LESS_THAN;
                break;
            case LESS_THAN_OR_EQUAL:
                operatorType = LESS_THAN_OR_EQUAL;
                break;
            case GREATER_THAN:
                operatorType = GREATER_THAN;
                break;
            case GREATER_THAN_OR_EQUAL:
                operatorType = GREATER_THAN_OR_EQUAL;
                break;
            case IS_DISTINCT_FROM:
                operatorType = IS_DISTINCT_FROM;
                break;
            default:
                throw new IllegalStateException("Unsupported comparison operator type: " + operator);
        }
        return operatorType;
    }

    @Override
    protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression actual, RowExpression expected)
    {
        if (expected instanceof CallExpression) {
            FunctionMetadata functionMetadata = metadata.getFunctionManager().getFunctionMetadata(((CallExpression) expected).getFunctionHandle());
            if (!functionMetadata.getOperatorType().isPresent() || !functionMetadata.getOperatorType().get().isArithmeticOperator()) {
                return false;
            }
            OperatorType expectedOperatorType = functionMetadata.getOperatorType().get();
            OperatorType actualOperatorType = getOperatorType(actual.getOperator());
            if (actualOperatorType.equals(expectedOperatorType)) {
                return process(actual.getLeft(), ((CallExpression) expected).getArguments().get(0)) && process(actual.getRight(), ((CallExpression) expected).getArguments().get(1));
            }
        }
        return false;
    }

    private static OperatorType getOperatorType(ArithmeticBinaryExpression.Operator operator)
    {
        OperatorType operatorType;
        switch (operator) {
            case ADD:
                operatorType = ADD;
                break;
            case SUBTRACT:
                operatorType = SUBTRACT;
                break;
            case MULTIPLY:
                operatorType = MULTIPLY;
                break;
            case DIVIDE:
                operatorType = DIVIDE;
                break;
            case MODULUS:
                operatorType = MODULUS;
                break;
            default:
                throw new IllegalStateException("Unknown arithmetic operator: " + operator);
        }
        return operatorType;
    }

    @Override
    protected Boolean visitGenericLiteral(GenericLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    @Override
    protected Boolean visitLongLiteral(LongLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    @Override
    protected Boolean visitDoubleLiteral(DoubleLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    @Override
    protected Boolean visitDecimalLiteral(DecimalLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    @Override
    protected Boolean visitBooleanLiteral(BooleanLiteral actual, RowExpression expected)
    {
        return compareLiteral(actual, expected);
    }

    private static String getValueFromLiteral(Node expression)
    {
        if (expression instanceof LongLiteral) {
            return String.valueOf(((LongLiteral) expression).getValue());
        }
        else if (expression instanceof BooleanLiteral) {
            return String.valueOf(((BooleanLiteral) expression).getValue());
        }
        else if (expression instanceof DoubleLiteral) {
            return String.valueOf(((DoubleLiteral) expression).getValue());
        }
        else if (expression instanceof DecimalLiteral) {
            return String.valueOf(((DecimalLiteral) expression).getValue());
        }
        else if (expression instanceof GenericLiteral) {
            return ((GenericLiteral) expression).getValue();
        }
        else {
            throw new IllegalArgumentException("Unsupported literal expression type: " + expression.getClass().getName());
        }
    }

    private Boolean compareLiteral(Node actual, RowExpression expected)
    {
        if (expected instanceof CallExpression && functionResolution.isCastFunction(((CallExpression) expected).getFunctionHandle())) {
            return getValueFromLiteral(actual).equals(String.valueOf(rowExpressionInterpreter(expected, metadata, session).evaluate()));
        }
        if (expected instanceof ConstantExpression) {
            return getValueFromLiteral(actual).equals(String.valueOf(LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) expected)));
        }
        return false;
    }

    @Override
    protected Boolean visitStringLiteral(StringLiteral actual, RowExpression expected)
    {
        if (expected instanceof ConstantExpression && expected.getType().getJavaType() == Slice.class) {
            String expectedString = (String) LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), (ConstantExpression) expected);
            return actual.getValue().equals(expectedString);
        }
        return false;
    }

    @Override
    protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression actual, RowExpression expected)
    {
        if (expected instanceof SpecialFormExpression) {
            SpecialFormExpression expectedLogicalBinary = (SpecialFormExpression) expected;
            if ((actual.getOperator() == OR && expectedLogicalBinary.getForm() == SpecialFormExpression.Form.OR) ||
                    (actual.getOperator() == AND && expectedLogicalBinary.getForm() == SpecialFormExpression.Form.AND)) {
                return process(actual.getLeft(), expectedLogicalBinary.getArguments().get(0)) &&
                        process(actual.getRight(), expectedLogicalBinary.getArguments().get(1));
            }
        }
        return false;
    }

    @Override
    protected Boolean visitBetweenPredicate(BetweenPredicate actual, RowExpression expected)
    {
        if (expected instanceof CallExpression && functionResolution.isBetweenFunction(((CallExpression) expected).getFunctionHandle())) {
            return process(actual.getValue(), ((CallExpression) expected).getArguments().get(0)) &&
                    process(actual.getMin(), ((CallExpression) expected).getArguments().get(1)) &&
                    process(actual.getMax(), ((CallExpression) expected).getArguments().get(2));
        }

        return false;
    }

    @Override
    protected Boolean visitNotExpression(NotExpression actual, RowExpression expected)
    {
        if (!(expected instanceof CallExpression) || !functionResolution.notFunction().equals(((CallExpression) expected).getFunctionHandle())) {
            return false;
        }
        return process(actual.getValue(), ((CallExpression) expected).getArguments().get(0));
    }

    @Override
    protected Boolean visitSymbolReference(SymbolReference actual, RowExpression expected)
    {
        if (!(expected instanceof VariableReferenceExpression)) {
            return false;
        }
        return symbolAliases.get((actual).getName()).getName().equals(((VariableReferenceExpression) expected).getName());
    }

    @Override
    protected Boolean visitCoalesceExpression(CoalesceExpression actual, RowExpression expected)
    {
        if (!(expected instanceof SpecialFormExpression) || !(((SpecialFormExpression) expected).getForm().equals(COALESCE))) {
            return false;
        }

        SpecialFormExpression expectedCoalesce = (SpecialFormExpression) expected;
        if (actual.getOperands().size() == expectedCoalesce.getArguments().size()) {
            boolean verified = true;
            for (int i = 0; i < actual.getOperands().size(); i++) {
                verified &= process(actual.getOperands().get(i), expectedCoalesce.getArguments().get(i));
            }
            return verified;
        }
        return false;
    }

    @Override
    protected Boolean visitSimpleCaseExpression(SimpleCaseExpression actual, RowExpression expected)
    {
        if (!(expected instanceof SpecialFormExpression && ((SpecialFormExpression) expected).getForm().equals(SWITCH))) {
            return false;
        }
        SpecialFormExpression expectedCase = (SpecialFormExpression) expected;
        if (!process(actual.getOperand(), expectedCase.getArguments().get(0))) {
            return false;
        }

        List<RowExpression> whenClauses;
        Optional<RowExpression> elseValue;
        RowExpression last = expectedCase.getArguments().get(expectedCase.getArguments().size() - 1);
        if (last instanceof SpecialFormExpression && ((SpecialFormExpression) last).getForm().equals(WHEN)) {
            whenClauses = expectedCase.getArguments().subList(1, expectedCase.getArguments().size());
            elseValue = Optional.empty();
        }
        else {
            whenClauses = expectedCase.getArguments().subList(1, expectedCase.getArguments().size() - 1);
            elseValue = Optional.of(last);
        }

        if (!process(actual.getWhenClauses(), whenClauses)) {
            return false;
        }

        return process(actual.getDefaultValue(), elseValue);
    }

    @Override
    protected Boolean visitWhenClause(WhenClause actual, RowExpression expected)
    {
        if (!(expected instanceof SpecialFormExpression && ((SpecialFormExpression) expected).getForm().equals(WHEN))) {
            return false;
        }
        SpecialFormExpression expectedWhenClause = (SpecialFormExpression) expected;

        return process(actual.getOperand(), ((SpecialFormExpression) expected).getArguments().get(0)) &&
                process(actual.getResult(), expectedWhenClause.getArguments().get(1));
    }

    @Override
    protected Boolean visitFunctionCall(FunctionCall actual, RowExpression expected)
    {
        if (!(expected instanceof CallExpression)) {
            return false;
        }
        CallExpression expectedFunction = (CallExpression) expected;

        if (!actual.getName().equals(QualifiedName.of(expectedFunction.getFunctionHandle().getSignature().getName()))) {
            return false;
        }

        return process(actual.getArguments(), expectedFunction.getArguments());
    }

    @Override
    protected Boolean visitNullLiteral(NullLiteral node, RowExpression expected)
    {
        return expected instanceof ConstantExpression && ((ConstantExpression) expected).getValue() == null;
    }

    private <T extends Node> boolean process(List<T> actuals, List<RowExpression> expecteds)
    {
        if (actuals.size() != expecteds.size()) {
            return false;
        }
        for (int i = 0; i < actuals.size(); i++) {
            if (!process(actuals.get(i), expecteds.get(i))) {
                return false;
            }
        }
        return true;
    }

    private <T extends Node> boolean process(Optional<T> actual, Optional<RowExpression> expected)
    {
        if (actual.isPresent() != expected.isPresent()) {
            return false;
        }
        if (actual.isPresent()) {
            return process(actual.get(), expected.get());
        }
        return true;
    }
}

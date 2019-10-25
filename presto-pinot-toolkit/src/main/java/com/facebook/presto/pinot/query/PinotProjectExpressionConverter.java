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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.pinot.PinotSessionProperties;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Origin;
import com.facebook.presto.pinot.query.PinotQueryGeneratorContext.Selection;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static com.facebook.presto.pinot.query.PinotExpression.derived;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Convert {@link com.facebook.presto.spi.relation.RowExpression} in project into Pinot compliant expression text
 */
class PinotProjectExpressionConverter
        implements RowExpressionVisitor<PinotExpression, Map<VariableReferenceExpression, Selection>>
{
    // Pinot does not support modulus yet
    private static final Map<String, String> PRESTO_TO_PINOT_OPERATORS = ImmutableMap.of(
            "-", "SUB",
            "+", "ADD",
            "*", "MULT",
            "/", "DIV");
    private static final String FROM_UNIXTIME = "from_unixtime";

    private static final Set<String> TIME_EQUIVALENT_TYPES = ImmutableSet.of(StandardTypes.BIGINT, StandardTypes.INTEGER, StandardTypes.TINYINT, StandardTypes.SMALLINT);

    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final StandardFunctionResolution standardFunctionResolution;
    private final ConnectorSession session;

    public PinotProjectExpressionConverter(
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            ConnectorSession session)
    {
        this.typeManager = requireNonNull(typeManager, "type manager");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.session = requireNonNull(session, "session is null");
    }

    private static String getStringFromConstant(RowExpression expression)
    {
        if (expression instanceof ConstantExpression) {
            Object value = ((ConstantExpression) expression).getValue();
            if (value instanceof String) {
                return (String) value;
            }
            if (value instanceof Slice) {
                return ((Slice) value).toStringUtf8();
            }
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Expected string literal but found " + expression);
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    // Copied from com.facebook.presto.sql.planner.LiteralInterpreter.evaluate
    public static String getLiteralAsString(ConstantExpression node)
    {
        Type type = node.getType();

        if (node.getValue() == null) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), String.format("Null constant expression %s with value of type %s", node, type));
        }
        if (type instanceof BooleanType) {
            return String.valueOf(((Boolean) node.getValue()).booleanValue());
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            Number number = (Number) node.getValue();
            return format("%d", number.longValue());
        }
        if (type instanceof DoubleType) {
            return node.getValue().toString();
        }
        if (type instanceof RealType) {
            Long number = (Long) node.getValue();
            return format("%f", intBitsToFloat(number.intValue()));
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                checkState(node.getValue() instanceof Long);
                return decodeDecimal(BigInteger.valueOf((long) node.getValue()), decimalType).toString();
            }
            checkState(node.getValue() instanceof Slice);
            Slice value = (Slice) node.getValue();
            return decodeDecimal(decodeUnscaledValue(value), decimalType).toString();
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return "'" + ((Slice) node.getValue()).toStringUtf8() + "'";
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), String.format("Cannot handle the constant expression %s with value of type %s", node, type));
    }

    @Override
    public PinotExpression visitVariableReference(
            VariableReferenceExpression reference,
            Map<VariableReferenceExpression, Selection> context)
    {
        Selection input = requireNonNull(context.get(reference), format("Input column %s does not exist in the input", reference));
        return new PinotExpression(input.getDefinition(), input.getOrigin());
    }

    private CallExpression getExpressionAsFunction(
            RowExpression originalExpression,
            RowExpression expression)
    {
        if (expression instanceof CallExpression) {
            CallExpression call = (CallExpression) expression;
            if (standardFunctionResolution.isCastFunction(call.getFunctionHandle())) {
                if (isImplicitCast(call.getArguments().get(0).getType(), call.getType())) {
                    return getExpressionAsFunction(originalExpression, call.getArguments().get(0));
                }
            }
            else {
                return call;
            }
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Could not dig function out of expression: " + originalExpression + ", inside of " + expression);
    }

    private PinotExpression handleDateTruncationViaDateTimeConvert(
            CallExpression function,
            Map<VariableReferenceExpression, Selection> context)
    {
        // Convert SQL standard function `DATE_TRUNC(INTERVAL, DATE/TIMESTAMP COLUMN)` to
        // Pinot's equivalent function `dateTimeConvert(columnName, inputFormat, outputFormat, outputGranularity)`
        // Pinot doesn't have a DATE/TIMESTAMP type. That means the input column (second argument) has been converted from numeric type to DATE/TIMESTAMP using one of the
        // conversion functions in SQL. First step is find the function and find its input column units (seconds, secondsSinceEpoch etc.)
        RowExpression timeInputParameter = function.getArguments().get(1);
        String inputColumn;
        String inputFormat;

        CallExpression timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter);
        switch (timeConversion.getDisplayName().toLowerCase(ENGLISH)) {
            case FROM_UNIXTIME:
                inputColumn = timeConversion.getArguments().get(0).accept(this, context).getDefinition();
                inputFormat = "'1:SECONDS:EPOCH'";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getDisplayName());
        }

        String outputFormat = "'1:MILLISECONDS:EPOCH'";
        String outputGranularity;

        RowExpression intervalParameter = function.getArguments().get(0);
        if (!(intervalParameter instanceof ConstantExpression)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(),
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        String value = getStringFromConstant(intervalParameter);
        switch (value) {
            case "second":
                outputGranularity = "'1:SECONDS'";
                break;
            case "minute":
                outputGranularity = "'1:MINUTES'";
                break;
            case "hour":
                outputGranularity = "'1:HOURS'";
                break;
            case "day":
                outputGranularity = "'1:DAYS'";
                break;
            case "week":
                outputGranularity = "'1:WEEKS'";
                break;
            case "month":
                outputGranularity = "'1:MONTHS'";
                break;
            case "quarter":
                outputGranularity = "'1:QUARTERS'";
                break;
            case "year":
                outputGranularity = "'1:YEARS'";
                break;
            default:
                throw new PinotException(
                        PINOT_UNSUPPORTED_EXPRESSION,
                        Optional.empty(),
                        "interval in date_trunc is not supported: " + value);
        }

        return derived("dateTimeConvert(" + inputColumn + ", " + inputFormat + ", " + outputFormat + ", " + outputGranularity + ")");
    }

    private PinotExpression handleDateTruncationViaDateTruncation(
            CallExpression function,
            Map<VariableReferenceExpression, Selection> context)
    {
        RowExpression timeInputParameter = function.getArguments().get(1);
        String inputColumn;
        String inputTimeZone;
        String inputFormat;

        CallExpression timeConversion = getExpressionAsFunction(timeInputParameter, timeInputParameter);
        switch (timeConversion.getDisplayName().toLowerCase(ENGLISH)) {
            case FROM_UNIXTIME:
                inputColumn = timeConversion.getArguments().get(0).accept(this, context).getDefinition();
                inputTimeZone = timeConversion.getArguments().size() > 1 ? getStringFromConstant(timeConversion.getArguments().get(1)) : DateTimeZone.UTC.getID();
                inputFormat = "seconds";
                break;
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "not supported: " + timeConversion.getDisplayName());
        }

        RowExpression intervalParameter = function.getArguments().get(0);
        if (!(intervalParameter instanceof ConstantExpression)) {
            throw new PinotException(
                    PINOT_UNSUPPORTED_EXPRESSION,
                    Optional.empty(),
                    "interval unit in date_trunc is not supported: " + intervalParameter);
        }

        return derived("dateTrunc(" + inputColumn + "," + inputFormat + ", " + inputTimeZone + ", " + getStringFromConstant(intervalParameter) + ")");
    }

    @Override
    public PinotExpression visitConstant(
            ConstantExpression literal,
            Map<VariableReferenceExpression, Selection> context)
    {
        return new PinotExpression(getLiteralAsString(literal), Origin.LITERAL);
    }

    @Override
    public PinotExpression visitLambda(
            LambdaDefinitionExpression lambda,
            Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Pinot does not support lambda " + lambda);
    }

    private boolean isImplicitCast(Type inputType, Type resultType)
    {
        if (typeManager.canCoerce(inputType, resultType)) {
            return true;
        }
        return resultType.getTypeSignature().getBase().equals(StandardTypes.TIMESTAMP) && TIME_EQUIVALENT_TYPES.contains(inputType.getTypeSignature().getBase());
    }

    private PinotExpression handleArithmeticExpression(
            CallExpression expression,
            OperatorType operatorType,
            Map<VariableReferenceExpression, Selection> context)
    {
        List<RowExpression> arguments = expression.getArguments();
        if (arguments.size() == 1) {
            String prefix = operatorType == OperatorType.NEGATION ? "-" : "";
            return derived(prefix + arguments.get(0).accept(this, context).getDefinition());
        }
        if (arguments.size() == 2) {
            PinotExpression left = arguments.get(0).accept(this, context);
            PinotExpression right = arguments.get(1).accept(this, context);
            String prestoOperator = operatorType.getOperator();
            String pinotOperator = PRESTO_TO_PINOT_OPERATORS.get(prestoOperator);
            if (pinotOperator == null) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported binary expression " + prestoOperator);
            }
            return derived(format("%s(%s, %s)", pinotOperator, left.getDefinition(), right.getDefinition()));
        }
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("Don't know how to interpret %s as an arithmetic expression", expression));
    }

    private PinotExpression handleCast(
            CallExpression cast,
            Map<VariableReferenceExpression, Selection> context)
    {
        if (cast.getArguments().size() == 1) {
            RowExpression input = cast.getArguments().get(0);
            Type expectedType = cast.getType();
            if (isImplicitCast(input.getType(), expectedType)) {
                return input.accept(this, context);
            }
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Non implicit casts not supported: " + cast);
        }

        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("This type of CAST operator not supported. Received: %s", cast));
    }

    private PinotExpression handleFunction(
            CallExpression function,
            Map<VariableReferenceExpression, Selection> context)
    {
        switch (function.getDisplayName().toLowerCase(ENGLISH)) {
            case "date_trunc":
                boolean useDateTruncation = PinotSessionProperties.isUseDateTruncation(session);
                return useDateTruncation ?
                        handleDateTruncationViaDateTruncation(function, context) :
                        handleDateTruncationViaDateTimeConvert(function, context);
            default:
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), format("function %s not supported yet", function.getDisplayName()));
        }
    }

    @Override
    public PinotExpression visitCall(
            CallExpression call,
            Map<VariableReferenceExpression, Selection> context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (standardFunctionResolution.isNotFunction(functionHandle) || standardFunctionResolution.isBetweenFunction(functionHandle)) {
            throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Unsupported function in pinot aggregation: " + functionHandle);
        }
        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            return handleCast(call, context);
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(call.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType.isArithmeticOperator()) {
                return handleArithmeticExpression(call, operatorType, context);
            }
            if (operatorType.isComparisonOperator()) {
                throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Comparison operator not supported: " + call);
            }
        }
        return handleFunction(call, context);
    }

    @Override
    public PinotExpression visitInputReference(
            InputReferenceExpression reference,
            Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Input reference not supported: " + reference);
    }

    @Override
    public PinotExpression visitSpecialForm(
            SpecialFormExpression specialForm,
            Map<VariableReferenceExpression, Selection> context)
    {
        throw new PinotException(PINOT_UNSUPPORTED_EXPRESSION, Optional.empty(), "Special form not supported: " + specialForm);
    }
}

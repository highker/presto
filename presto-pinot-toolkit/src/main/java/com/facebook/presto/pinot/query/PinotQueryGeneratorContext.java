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

import com.facebook.presto.pinot.PinotColumnHandle;
import com.facebook.presto.pinot.PinotConfig;
import com.facebook.presto.pinot.PinotErrorCode;
import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.pinot.PinotPushdownUtils.checkSupported;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

/**
 * Encapsulates the components needed to construct a PQL query and provides methods to update the current context with new operations.
 */
public class PinotQueryGeneratorContext
{
    public static final String TIME_BOUNDARY_FILTER_TEMPLATE = "__TIME_BOUNDARY_FILTER_TEMPLATE__";
    public static final String TABLE_NAME_SUFFIX_TEMPLATE = "__TABLE_NAME_SUFFIX_TEMPLATE__";
    // Fields defining the query
    // order map that maps the column definition in terms of input relation column(s)
    private final LinkedHashMap<VariableReferenceExpression, Selection> selections;
    private final LinkedHashSet<VariableReferenceExpression> groupByColumns;
    private final LinkedHashMap<VariableReferenceExpression, SortOrder> topNColumnOrderingMap;
    private final Set<VariableReferenceExpression> hiddenColumnSet;
    private final String from;
    private final Optional<String> filter;
    private final OptionalInt limit;
    private final int aggregations;

    public boolean isQueryShort(int nonAggregateRowLimit)
    {
        return hasAggregation() || limit.orElse(Integer.MAX_VALUE) < nonAggregateRowLimit;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("selections", selections)
                .add("groupByColumns", groupByColumns)
                .add("hiddenColumnSet", hiddenColumnSet)
                .add("from", from)
                .add("filter", filter)
                .add("limit", limit)
                .add("aggregations", aggregations)
                .toString();
    }

    PinotQueryGeneratorContext(
            LinkedHashMap<VariableReferenceExpression, Selection> selections,
            String from)
    {
        this(
                selections,
                from,
                Optional.empty(),
                0,
                new LinkedHashSet<>(),
                new LinkedHashMap<>(),
                OptionalInt.empty(),
                new HashSet<>());
    }

    private PinotQueryGeneratorContext(
            LinkedHashMap<VariableReferenceExpression, Selection> selections,
            String from,
            Optional<String> filter,
            int aggregations,
            LinkedHashSet<VariableReferenceExpression> groupByColumns,
            LinkedHashMap<VariableReferenceExpression, SortOrder> topNColumnOrderingMap,
            OptionalInt limit,
            Set<VariableReferenceExpression> hiddenColumnSet)
    {
        this.selections = new LinkedHashMap<>(requireNonNull(selections, "selections can't be null"));
        this.from = requireNonNull(from, "source can't be null");
        this.aggregations = aggregations;
        this.groupByColumns = new LinkedHashSet<>(requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available"));
        this.topNColumnOrderingMap = new LinkedHashMap<>(requireNonNull(topNColumnOrderingMap, "topNColumnOrderingMap can't be null. It could be empty if not available"));
        this.filter = requireNonNull(filter, "filter is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.hiddenColumnSet = requireNonNull(hiddenColumnSet, "hidden column set is null");
    }

    /**
     * Apply the given filter to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withFilter(String filter)
    {
        checkSupported(!hasFilter(), "There already exists a filter. Pinot doesn't support filters at multiple levels");
        checkSupported(!hasAggregation(), "Pinot doesn't support filtering the results of aggregation");
        checkSupported(!hasLimit(), "Pinot doesn't support filtering on top of the limit");
        return new PinotQueryGeneratorContext(
                selections,
                from,
                Optional.of(filter),
                aggregations,
                groupByColumns,
                topNColumnOrderingMap,
                limit,
                hiddenColumnSet);
    }

    /**
     * Apply the aggregation to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withAggregation(
            LinkedHashMap<VariableReferenceExpression, Selection> newSelections,
            LinkedHashSet<VariableReferenceExpression> groupByColumns,
            int aggregations,
            HashSet<VariableReferenceExpression> hiddenColumnSet)
    {
        // there is only one aggregation supported.
        checkSupported(!hasAggregation(), "Pinot doesn't support aggregation on top of the aggregated data");
        checkSupported(!hasLimit(), "Pinot doesn't support aggregation on top of the limit");
        checkSupported(aggregations > 0, "Invalid number of aggregations");
        return new PinotQueryGeneratorContext(newSelections, from, filter, aggregations, groupByColumns, topNColumnOrderingMap, limit, hiddenColumnSet);
    }

    /**
     * Apply new selections/project to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withProject(LinkedHashMap<VariableReferenceExpression, Selection> newSelections)
    {
        checkSupported(groupByColumns.isEmpty(), "Pinot doesn't yet support new selections on top of the grouped by data");
        return new PinotQueryGeneratorContext(
                newSelections,
                from,
                filter,
                aggregations,
                groupByColumns,
                topNColumnOrderingMap,
                limit,
                hiddenColumnSet);
    }

    private static int checkForValidLimit(long limit)
    {
        if (limit <= 0 || limit > Integer.MAX_VALUE) {
            throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Limit " + limit + " not supported: Limit is not being pushed down");
        }
        return toIntExact(limit);
    }

    /**
     * Apply limit to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withLimit(long limit)
    {
        int intLimit = checkForValidLimit(limit);
        checkSupported(!hasLimit(), "Limit already exists. Pinot doesn't support limit on top of another limit");
        return new PinotQueryGeneratorContext(
                selections,
                from,
                filter,
                aggregations,
                groupByColumns,
                topNColumnOrderingMap,
                OptionalInt.of(intLimit),
                hiddenColumnSet);
    }

    /**
     * Apply order by to current context and return the updated context. Throws error for invalid operations.
     */
    public PinotQueryGeneratorContext withTopN(LinkedHashMap<VariableReferenceExpression, SortOrder> orderByColumnOrderingMap, long limit)
    {
        checkSupported(!hasLimit(), "Limit already exists. Pinot doesn't support order by limit on top of another limit");
        checkSupported(!hasAggregation(), "Pinot doesn't support ordering on top of the aggregated data");
        int intLimit = checkForValidLimit(limit);
        return new PinotQueryGeneratorContext(
                selections,
                from,
                filter,
                aggregations,
                groupByColumns,
                orderByColumnOrderingMap,
                OptionalInt.of(intLimit),
                hiddenColumnSet);
    }

    private boolean hasFilter()
    {
        return filter.isPresent();
    }

    private boolean hasLimit()
    {
        return limit.isPresent();
    }

    private boolean hasAggregation()
    {
        return aggregations > 0;
    }

    private boolean hasOrderBy()
    {
        return !topNColumnOrderingMap.isEmpty();
    }

    public LinkedHashMap<VariableReferenceExpression, Selection> getSelections()
    {
        return selections;
    }

    public Set<VariableReferenceExpression> getHiddenColumnSet()
    {
        return hiddenColumnSet;
    }

    /**
     * Convert the current context to a PQL
     */
    public PinotQueryGenerator.GeneratedPql toQuery(PinotConfig pinotConfig, boolean preferBrokerQueries, boolean isQueryShort)
    {
        boolean forBroker = preferBrokerQueries && isQueryShort;
        if (!pinotConfig.isAllowMultipleAggregations() && aggregations > 1 && !groupByColumns.isEmpty()) {
            throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Multiple aggregates in the presence of group by is forbidden");
        }

        if (hasLimit() && aggregations > 1 && !groupByColumns.isEmpty()) {
            throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Multiple aggregates in the presence of group by and limit is forbidden");
        }

        String expressions = selections.entrySet().stream()
                .filter(s -> !groupByColumns.contains(s.getKey())) // remove the group by columns from the query as Pinot barfs if the group by column is an expression
                .map(s -> s.getValue().getDefinition())
                .collect(Collectors.joining(", "));

        String query = "SELECT " + expressions + " FROM " + from + (forBroker ? "" : TABLE_NAME_SUFFIX_TEMPLATE);
        if (filter.isPresent()) {
            String filterString = filter.get();
            String afterWhereString;
            if (!forBroker) {
                // this is hack!!!. Ideally we want to clone the scan pipeline and create/update the filter in the scan pipeline to contain this filter and
                // at the same time add the time column to scan so that the query generator doesn't fail when it looks up the time column in scan output columns
                afterWhereString = filterString + TIME_BOUNDARY_FILTER_TEMPLATE;
            }
            else {
                afterWhereString = filterString;
            }
            query += " WHERE " + afterWhereString;
        }
        else if (!forBroker) {
            query += TIME_BOUNDARY_FILTER_TEMPLATE;
        }

        if (!groupByColumns.isEmpty()) {
            String groupByExpr = groupByColumns.stream().map(x -> selections.get(x).getDefinition()).collect(Collectors.joining(", "));
            query = query + " GROUP BY " + groupByExpr;
        }

        if (hasOrderBy()) {
            String orderByExpressions = topNColumnOrderingMap.entrySet().stream().map(entry -> selections.get(entry.getKey()).getDefinition() + (entry.getValue().isAscending() ? "" : " DESC")).collect(Collectors.joining(", "));
            query = query + " ORDER BY " + orderByExpressions;
        }
        // Rules for limit:
        // - If its a selection query:
        //      + given limit or configured limit
        // - Else if has group by:
        //      + ensure that only one aggregation
        //      + default limit or configured top limit
        // - Fail if limit is invalid

        String limitKeyWord = "";
        int queryLimit = -1;

        if (!hasAggregation()) {
            if (!limit.isPresent() && forBroker) {
                throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.empty(), "Broker non aggregate queries have to have a limit");
            }
            else {
                queryLimit = limit.orElseGet(pinotConfig::getLimitLargeForSegment);
            }
            limitKeyWord = "LIMIT";
        }
        else if (!groupByColumns.isEmpty()) {
            limitKeyWord = "TOP";
            if (limit.isPresent()) {
                if (aggregations > 1) {
                    throw new PinotException(PinotErrorCode.PINOT_QUERY_GENERATOR_FAILURE, Optional.of(query),
                            "Pinot has weird semantics with group by and multiple aggregation functions and limits");
                }
                else {
                    queryLimit = limit.getAsInt();
                }
            }
            else {
                queryLimit = pinotConfig.getTopNLarge();
            }
        }

        if (!limitKeyWord.isEmpty()) {
            query += " " + limitKeyWord + " " + queryLimit;
        }

        List<PinotColumnHandle> columnHandles = ImmutableList.copyOf(getAssignments().values());
        return new PinotQueryGenerator.GeneratedPql(from, query, getIndicesMappingFromPinotSchemaToPrestoSchema(query, columnHandles), groupByColumns.size(), filter != null, isQueryShort);
    }

    private List<Integer> getIndicesMappingFromPinotSchemaToPrestoSchema(String query, List<PinotColumnHandle> handles)
    {
        LinkedHashMap<VariableReferenceExpression, Selection> expressionsInPinotOrder = new LinkedHashMap<>();
        for (VariableReferenceExpression groupByColumn : groupByColumns) {
            Selection groupByColumnDefinition = selections.get(groupByColumn);
            if (groupByColumnDefinition == null) {
                throw new IllegalStateException(format(
                        "Group By column (%s) definition not found in input selections: %s",
                        groupByColumn,
                        Joiner.on(",").withKeyValueSeparator(":").join(selections)));
            }
            expressionsInPinotOrder.put(groupByColumn, groupByColumnDefinition);
        }
        expressionsInPinotOrder.putAll(selections);

        checkSupported(
                handles.size() == expressionsInPinotOrder.keySet().stream().filter(key -> !hiddenColumnSet.contains(key)).count(),
                "Expected returned expressions %s to match selections %s",
                Joiner.on(",").withKeyValueSeparator(":").join(expressionsInPinotOrder), Joiner.on(",").join(handles));

        Map<VariableReferenceExpression, Integer> nameToIndex = new HashMap<>();
        for (int i = 0; i < handles.size(); i++) {
            PinotColumnHandle columnHandle = handles.get(i);
            VariableReferenceExpression columnName = new VariableReferenceExpression(columnHandle.getColumnName().toLowerCase(ENGLISH), columnHandle.getDataType());
            Integer previous = nameToIndex.put(columnName, i);
            if (previous != null) {
                throw new PinotException(PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.of(query), format("Expected Pinot column handle %s to occur only once, but we have: %s", columnName, Joiner.on(",").join(handles)));
            }
        }

        ImmutableList.Builder<Integer> outputIndices = ImmutableList.builder();
        for (Map.Entry<VariableReferenceExpression, Selection> expression : expressionsInPinotOrder.entrySet()) {
            Integer index = nameToIndex.get(expression.getKey());
            if (hiddenColumnSet.contains(expression.getKey())) {
                index = -1; // negative output index means to skip this value returned by pinot at query time
            }
            if (index == null) {
                throw new PinotException(
                        PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION, Optional.of(query),
                        format(
                                "Expected to find a Pinot column handle for the expression %s, but we have %s",
                                expression,
                                Joiner.on(",").withKeyValueSeparator(":").join(nameToIndex)));
            }
            outputIndices.add(index);
        }
        return outputIndices.build();
    }

    public LinkedHashMap<VariableReferenceExpression, PinotColumnHandle> getAssignments()
    {
        LinkedHashMap<VariableReferenceExpression, PinotColumnHandle> result = new LinkedHashMap<>();
        selections.entrySet().stream().filter(e -> !hiddenColumnSet.contains(e.getKey())).forEach(entry -> {
            VariableReferenceExpression variable = entry.getKey();
            Selection selection = entry.getValue();
            PinotColumnHandle handle = selection.getOrigin() == Origin.TABLE_COLUMN ? new PinotColumnHandle(selection.getDefinition(), variable.getType(), PinotColumnHandle.PinotColumnType.REGULAR) : new PinotColumnHandle(variable, PinotColumnHandle.PinotColumnType.DERIVED);
            result.put(variable, handle);
        });
        return result;
    }

    public PinotQueryGeneratorContext withOutputColumns(List<VariableReferenceExpression> outputColumns)
    {
        LinkedHashMap<VariableReferenceExpression, Selection> newSelections = new LinkedHashMap<>();
        outputColumns.forEach(o -> newSelections.put(o, requireNonNull(selections.get(o), String.format("Cannot find the selection %s in the original context %s", o, this))));

        // Hidden columns flow as is from the previous
        selections.entrySet().stream().filter(e -> hiddenColumnSet.contains(e.getKey())).forEach(e -> newSelections.put(e.getKey(), e.getValue()));
        return new PinotQueryGeneratorContext(newSelections, from, filter, aggregations, groupByColumns, topNColumnOrderingMap, limit, hiddenColumnSet);
    }

    /**
     * Where is the selection/projection originated from
     */
    public enum Origin
    {
        TABLE_COLUMN, // refers to direct column in table
        DERIVED, // expression is derived from one or more input columns or a combination of input columns and literals
        LITERAL, // derived from literal
    }

    // Projected/selected column definition in query
    public static class Selection
    {
        private final String definition;
        private final Origin origin;

        public Selection(String definition, Origin origin)
        {
            this.definition = definition;
            this.origin = origin;
        }

        public String getDefinition()
        {
            return definition;
        }

        public Origin getOrigin()
        {
            return origin;
        }

        @Override
        public String toString()
        {
            return definition;
        }
    }
}

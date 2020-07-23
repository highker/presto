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
package com.facebook.presto.server;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.execution.SqlQueryExecution;
import com.facebook.presto.execution.StageExecutionState;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterPlaceholder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

@ThreadSafe
public class DynamicFilterService
{
    private final Map<SourceDescriptor, Domain> dynamicFilterSummaries = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final Map<QueryId, Supplier<List<StageInfo>>> queries = new HashMap<>();

    private final ScheduledExecutorService collectDynamicFiltersExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("DynamicFilterService"));

    @Inject
    public DynamicFilterService(TaskManagerConfig taskConfig)
    {
        collectDynamicFiltersExecutor.scheduleWithFixedDelay(this::collectDynamicFilters, 0, taskConfig.getStatusRefreshMaxWait().toMillis(), MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        collectDynamicFiltersExecutor.shutdownNow();
    }

    public void registerQuery(SqlQueryExecution sqlQueryExecution)
    {
        // register query only if it contains dynamic filters
        boolean hasDynamicFilters = PlanNodeSearcher.searchFrom(sqlQueryExecution.getQueryPlan().getRoot())
                .where(node -> node instanceof JoinNode && !((JoinNode) node).getDynamicFilters().isEmpty())
                .matches();
        if (hasDynamicFilters) {
            registerQuery(sqlQueryExecution.getQueryId(), sqlQueryExecution::getAllStages);
        }
    }

    @VisibleForTesting
    public synchronized void registerQuery(QueryId queryId, Supplier<List<StageInfo>> stageInfoSupplier)
    {
        queries.putIfAbsent(queryId, stageInfoSupplier);
    }

    public synchronized void removeQuery(QueryId queryId)
    {
        dynamicFilterSummaries.keySet().removeIf(sourceDescriptor -> sourceDescriptor.getQueryId().equals(queryId));
        queries.remove(queryId);
    }

    @VisibleForTesting
    public synchronized void collectDynamicFilters()
    {
        for (Map.Entry<QueryId, Supplier<List<StageInfo>>> entry : queries.entrySet()) {
            System.out.println("james try to collectDynamicFilters");
            QueryId queryId = entry.getKey();
            for (StageInfo stageInfo : entry.getValue().get()) {
                StageExecutionState stageState = stageInfo.getLatestAttemptExecutionInfo().getState();
                // wait until stage has finished scheduling tasks
                if (stageState.canScheduleMoreTasks()) {
                    continue;
                }
                List<TaskInfo> tasks = stageInfo.getLatestAttemptExecutionInfo().getTasks();
                Map<String, List<Domain>> stageDynamicFilterDomains = tasks.stream()
                        .map(taskInfo -> taskInfo.getTaskStatus().getDynamicFilterDomains())
                        .flatMap(taskDomains -> taskDomains.entrySet().stream())
                        .collect(groupingBy(Map.Entry::getKey, mapping(Map.Entry::getValue, toList())));

                stageDynamicFilterDomains.entrySet().stream()
                        // check if all tasks of a dynamic filter source have reported dynamic filter summary
                        .filter(stageDomains -> stageDomains.getValue().size() == tasks.size())
                        .forEach(stageDomains -> {
                            dynamicFilterSummaries.put(
                                SourceDescriptor.of(queryId, stageDomains.getKey()),
                                Domain.union(stageDomains.getValue()));
                            System.out.println("james collectDynamicFilters");
                        });
            }
        }
    }

    public Supplier<TupleDomain<ColumnHandle>> createDynamicFilterSupplier(QueryId queryId, List<DynamicFilterPlaceholder> dynamicFilters, Map<VariableReferenceExpression, ColumnHandle> columnHandles)
    {
        System.out.println("james createDynamicFilterSupplier");
        Map<String, ColumnHandle> sourceColumnHandles = extractSourceColumnHandles(dynamicFilters, columnHandles);

        return () -> dynamicFilters.stream()
                .map(filter -> getSummary(queryId, filter.getId())
                        .map(summary -> translateSummaryToTupleDomain(filter.getId(), summary, sourceColumnHandles)))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .reduce(TupleDomain.all(), TupleDomain::intersect);
    }

    @VisibleForTesting
    Optional<Domain> getSummary(QueryId queryId, String filterId)
    {
        return Optional.ofNullable(dynamicFilterSummaries.get(SourceDescriptor.of(queryId, filterId)));
    }

    @Immutable
    private static class SourceDescriptor
    {
        private final QueryId queryId;
        private final String filterId;

        public static SourceDescriptor of(QueryId queryId, String filterId)
        {
            return new SourceDescriptor(queryId, filterId);
        }

        private SourceDescriptor(QueryId queryId, String filterId)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.filterId = requireNonNull(filterId, "filterId is null");
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public String getFilterId()
        {
            return filterId;
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == this) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }

            SourceDescriptor sourceDescriptor = (SourceDescriptor) other;

            return Objects.equals(queryId, sourceDescriptor.queryId) &&
                    Objects.equals(filterId, sourceDescriptor.filterId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queryId, filterId);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("queryId", queryId)
                    .add("filterId", filterId)
                    .toString();
        }
    }

    private static TupleDomain<ColumnHandle> translateSummaryToTupleDomain(String filterId, Domain summary, Map<String, ColumnHandle> sourceColumnHandles)
    {
        if (summary.isNone()) {
            return TupleDomain.none();
        }
        ColumnHandle sourceColumnHandle = requireNonNull(sourceColumnHandles.get(filterId), () -> format("Source column handle for dynamic filter %s is null", filterId));
        return TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>builder()
                .put(sourceColumnHandle, summary)
                .build());
    }

    private static Map<String, ColumnHandle> extractSourceColumnHandles(List<DynamicFilterPlaceholder> dynamicFilters, Map<VariableReferenceExpression, ColumnHandle> columnHandles)
    {
        return dynamicFilters.stream()
                .collect(toImmutableMap(
                        DynamicFilterPlaceholder::getId,
                        placeholder -> columnHandles.get((VariableReferenceExpression) placeholder.getInput())));
    }
}

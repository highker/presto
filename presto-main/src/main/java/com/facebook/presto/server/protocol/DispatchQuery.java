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
package com.facebook.presto.server.protocol;

import com.facebook.presto.Session;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.server.ForStatementResource;
import com.facebook.presto.spi.QueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.core.UriInfo;

import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DispatchQuery
        extends AbstractQuery
{
    private final Executor redirectProcessorExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    @Nullable
    @GuardedBy("this")
    private QueryResults redirectResults;

    @GuardedBy("this")
    private long updateCount;

    static DispatchQuery create(
            QueryId queryId,
            Session session,
            QueryManager queryManager,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        DispatchQuery result = new DispatchQuery(queryId, session, queryManager, dataProcessorExecutor, timeoutExecutor);

        // set redirect results obtained from the coordinator
        result.queryManager.addRedirectResultsListener(result.getQueryId(), queryResults -> result.redirectResults = requireNonNull(queryResults, "queryResults is null"));

        return result;
    }

    private DispatchQuery(
            QueryId queryId,
            Session session,
            QueryManager queryManager,
            Executor redirectProcessorExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        super(queryId, session, queryManager);
        this.redirectProcessorExecutor = requireNonNull(redirectProcessorExecutor, "redirectProcessorExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @Override
    public synchronized void dispose()
    {
        // no-op
    }

    @Override
    public synchronized ListenableFuture<QueryResults> waitForResults(OptionalLong token, UriInfo uriInfo, Duration wait)
    {
        if (redirectResults != null) {
            // update the query info in order not to miss a header
            updateInfo(queryManager.getQueryInfo(queryId));
            return immediateFuture(redirectResults);
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getRedirectResult(uriInfo), redirectProcessorExecutor);
    }

    private synchronized ListenableFuture<?> getFutureStateChange()
    {
        queryManager.recordHeartbeat(queryId);
        return queryManager.getQueryState(queryId).map(this::queryDoneFuture)
                .orElse(immediateFuture(null));
    }

    private synchronized QueryResults getRedirectResult(UriInfo uriInfo)
    {
        if (redirectResults != null) {
            // update the query info in order not to miss a header
            updateInfo(queryManager.getQueryInfo(queryId));
            return redirectResults;
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        updateInfo(queryInfo);
        updateCount++;

        // build the next uri to still connecting to the dispatcher
        return new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                null,
                uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString()).path("0").build(),
                ImmutableList.of(),
                null,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                queryInfo.getUpdateType(),
                updateCount);
    }

    private ListenableFuture<?> queryDoneFuture(QueryState currentState)
    {
        if (currentState.isDone()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(queryManager.getStateChange(queryId, currentState), this::queryDoneFuture);
    }

    private static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .build();
    }

    public static class DispatchQueryFactory
            implements QueryFactory<DispatchQuery>
    {
        private final Executor redirectProcessorExecutor;
        private final ScheduledExecutorService timeoutExecutor;

        @Inject
        DispatchQueryFactory(
                @ForStatementResource BoundedExecutor redirectProcessorExecutor,
                @ForStatementResource ScheduledExecutorService timeoutExecutor)
        {
            this.redirectProcessorExecutor = requireNonNull(redirectProcessorExecutor, "responseExecutor is null");
            this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        }

        @Override
        public DispatchQuery create(QueryId queryId, Session session, QueryManager queryManager)
        {
            return DispatchQuery.create(
                    queryId,
                    session,
                    queryManager,
                    redirectProcessorExecutor,
                    timeoutExecutor);
        }
    }
}

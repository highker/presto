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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.scheduler.SqlQueryScheduler;
import com.facebook.presto.memory.VersionedMemoryPoolId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.QueryType;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowGrants;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.presto.spi.resourceGroups.QueryType.DELETE;
import static com.facebook.presto.spi.resourceGroups.QueryType.DESCRIBE;
import static com.facebook.presto.spi.resourceGroups.QueryType.EXPLAIN;
import static com.facebook.presto.spi.resourceGroups.QueryType.INSERT;
import static com.facebook.presto.spi.resourceGroups.QueryType.SELECT;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public abstract class SqlQueryExecution
        implements QueryExecution
{
    final QueryStateMachine stateMachine;

    final Statement statement;

    final AtomicReference<SqlQueryScheduler> queryScheduler = new AtomicReference<>();

    public SqlQueryExecution(
            QueryId queryId,
            String query,
            Session session,
            URI self,
            Statement statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            ExecutorService queryExecutor)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryId)) {
            this.statement = requireNonNull(statement, "statement is null");

            requireNonNull(queryId, "queryId is null");
            requireNonNull(query, "query is null");
            requireNonNull(session, "session is null");
            requireNonNull(self, "self is null");
            this.stateMachine = QueryStateMachine.begin(queryId, query, session, self, false, transactionManager, accessControl, queryExecutor, metadata);

            // when the query finishes cache the final query info, and clear the reference to the output stage
            stateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }

                // query is now done, so abort any work that is still running
                SqlQueryScheduler scheduler = queryScheduler.get();
                if (scheduler != null) {
                    scheduler.abort();
                }
            });
        }
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            stateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    @Override
    public Optional<QueryType> getQueryType()
    {
        if (statement instanceof Query) {
            return Optional.of(SELECT);
        }
        else if (statement instanceof Explain) {
            return Optional.of(EXPLAIN);
        }
        else if (statement instanceof ShowCatalogs || statement instanceof ShowCreate || statement instanceof ShowFunctions ||
                statement instanceof ShowGrants || statement instanceof ShowPartitions || statement instanceof ShowSchemas ||
                statement instanceof ShowSession || statement instanceof ShowStats || statement instanceof ShowTables ||
                statement instanceof ShowColumns || statement instanceof DescribeInput || statement instanceof DescribeOutput) {
            return Optional.of(DESCRIBE);
        }
        else if (statement instanceof CreateTableAsSelect || statement instanceof Insert) {
            return Optional.of(INSERT);
        }
        else if (statement instanceof Delete) {
            return Optional.of(DELETE);
        }
        return Optional.empty();
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void fail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        stateMachine.transitionToFailed(cause);
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.addOutputInfoListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            // acquire reference to scheduler before checking finalQueryInfo, because
            // state change listener sets finalQueryInfo and then clears scheduler when
            // the query finishes.
            SqlQueryScheduler scheduler = queryScheduler.get();

            Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
            if (finalQueryInfo.isPresent()) {
                return finalQueryInfo.get();
            }

            return buildQueryInfo(scheduler);
        }
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Optional<ResourceGroupId> getResourceGroup()
    {
        return stateMachine.getResourceGroup();
    }

    @Override
    public void setResourceGroup(ResourceGroupId resourceGroupId)
    {
        stateMachine.setResourceGroup(resourceGroupId);
    }

    private QueryInfo buildQueryInfo(SqlQueryScheduler scheduler)
    {
        Optional<StageInfo> stageInfo = Optional.empty();
        if (scheduler != null) {
            stageInfo = Optional.ofNullable(scheduler.getStageInfo());
        }

        QueryInfo queryInfo = stateMachine.updateQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            // capture the final query state and drop reference to the scheduler
            queryScheduler.set(null);
        }

        return queryInfo;
    }
}

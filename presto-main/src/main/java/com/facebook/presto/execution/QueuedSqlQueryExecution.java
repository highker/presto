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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.transaction.TransactionManager;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static io.airlift.units.Duration.succinctNanos;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public final class QueuedSqlQueryExecution
        extends SqlQueryExecution
{
    private QueuedSqlQueryExecution(
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
        super(queryId, query, session, self, statement, transactionManager, metadata, accessControl, queryExecutor);
    }

    @Override
    public long getUserMemoryReservation()
    {
        return 0;
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return succinctNanos(0);
    }

    @Override
    public void start()
    {
        stateMachine.transitionToStarting();
    }

    @Override
    public void submitQuery()
    {
        stateMachine.transitionToSubmitted();
    }

    @Override
    public void finishQuery()
    {
        stateMachine.transitionToFinishing();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void pruneInfo()
    {
        // no-op
    }

    @Override
    public Plan getQueryPlan()
    {
        throw new UnsupportedOperationException();
    }

    public static class QueuedSqlQueryExecutionFactory
            implements QueryExecutionFactory<QueuedSqlQueryExecution>
    {
        private final Metadata metadata;
        private final AccessControl accessControl;
        private final TransactionManager transactionManager;
        private final LocationFactory locationFactory;
        private final ExecutorService queryExecutor;

        @Inject
        QueuedSqlQueryExecutionFactory(
                Metadata metadata,
                AccessControl accessControl,
                LocationFactory locationFactory,
                TransactionManager transactionManager,
                @ForQueryExecution ExecutorService queryExecutor)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
        }

        @Override
        public QueuedSqlQueryExecution createQueryExecution(QueryId queryId, String query, Session session, Statement statement, List<Expression> parameters, Optional<Runnable> dispatcherNotifier)
        {
            return new QueuedSqlQueryExecution(
                    queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    statement,
                    transactionManager,
                    metadata,
                    accessControl,
                    queryExecutor);
        }
    }
}

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
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.transaction.TransactionId;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

abstract class AbstractQuery
        implements Query
{
    private static final Logger log = Logger.get(AbstractQuery.class);

    final QueryManager queryManager;

    final QueryId queryId;
    final Session session;

    @GuardedBy("this")
    private Optional<String> setCatalog;

    @GuardedBy("this")
    private Optional<String> setSchema;

    @GuardedBy("this")
    private Map<String, String> setSessionProperties;

    @GuardedBy("this")
    private Set<String> resetSessionProperties;

    @GuardedBy("this")
    private Map<String, String> addedPreparedStatements;

    @GuardedBy("this")
    private Set<String> deallocatedPreparedStatements;

    @GuardedBy("this")
    private Optional<TransactionId> startedTransactionId;

    @GuardedBy("this")
    private boolean clearTransactionId;

    AbstractQuery(
            QueryId queryId,
            Session session,
            QueryManager queryManager)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.session = requireNonNull(session, "session is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @Override
    public void cancel()
    {
        queryManager.cancelQuery(queryId);
        dispose();
    }

    @Override
    public QueryId getQueryId()
    {
        return queryId;
    }

    @Override
    public synchronized Optional<String> getSetCatalog()
    {
        return setCatalog;
    }

    @Override
    public synchronized Optional<String> getSetSchema()
    {
        return setSchema;
    }

    @Override
    public synchronized Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    @Override
    public synchronized Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    @Override
    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    @Override
    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    @Override
    public synchronized Optional<TransactionId> getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @Override
    public synchronized boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    static QueryError toQueryError(QueryInfo queryInfo)
    {
        FailureInfo failure = queryInfo.getFailureInfo();
        if (failure == null) {
            QueryState state = queryInfo.getState();
            if ((!state.isDone()) || (state == QueryState.ACKNOWLEDGED) || (state == QueryState.FINISHED)) {
                return null;
            }
            log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
            failure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state))).toFailureInfo();
        }

        ErrorCode errorCode;
        if (queryInfo.getErrorCode() != null) {
            errorCode = queryInfo.getErrorCode();
        }
        else {
            errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
            log.warn("Failed query %s has no error code", queryInfo.getQueryId());
        }
        return new QueryError(
                failure.getMessage(),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                failure.getErrorLocation(),
                failure);
    }

    synchronized void updateInfo(QueryInfo queryInfo)
    {
        // update catalog and schema
        setCatalog = queryInfo.getSetCatalog();
        setSchema = queryInfo.getSetSchema();

        // update setSessionProperties
        setSessionProperties = queryInfo.getSetSessionProperties();
        resetSessionProperties = queryInfo.getResetSessionProperties();

        // update preparedStatements
        addedPreparedStatements = queryInfo.getAddedPreparedStatements();
        deallocatedPreparedStatements = queryInfo.getDeallocatedPreparedStatements();

        // update startedTransactionId
        startedTransactionId = queryInfo.getStartedTransactionId();
        clearTransactionId = queryInfo.isClearTransactionId();
    }
}

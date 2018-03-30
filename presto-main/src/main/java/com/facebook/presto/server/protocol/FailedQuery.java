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
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.UriInfo;

import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;

@ThreadSafe
public class FailedQuery
        extends AbstractQuery
{
    public static FailedQuery failed(QueryId queryId, Session session, QueryManager queryManager)
    {
        return new FailedQuery(queryId, session, queryManager);
    }

    private FailedQuery(QueryId queryId, Session session, QueryManager queryManager)
    {
        super(queryId, session, queryManager);
    }

    @Override
    public synchronized void dispose()
    {
        // no-op
    }

    @Override
    public synchronized ListenableFuture<QueryResults> waitForResults(OptionalLong token, UriInfo uriInfo, Duration wait)
    {
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        checkState(queryInfo.getFailureInfo() != null);

        updateInfo(queryInfo);

        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                null,
                null,
                null,
                null,
                StatementStats.builder()
                        .setState(queryInfo.getState().toString())
                        .setElapsedTimeMillis(queryInfo.getQueryStats().getElapsedTime().toMillis())
                        .build(),
                toQueryError(queryInfo),
                queryInfo.getUpdateType(),
                null);
        return immediateFuture(queryResults);
    }
}

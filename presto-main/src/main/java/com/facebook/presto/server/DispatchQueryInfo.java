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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.transaction.TransactionId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Lightweight version of QueryInfo. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@Immutable
public class DispatchQueryInfo
{
    private final QueryId queryId;
    private final QueryState queryState;
    private final Optional<TransactionId> startedTransactionId;
    private final boolean clearTransactionId;
    private final DateTime currentHeartBeat;

    @JsonCreator
    public DispatchQueryInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("queryState") QueryState queryState,
            @JsonProperty("startedTransactionId") Optional<TransactionId> startedTransactionId,
            @JsonProperty("clearTransactionId") boolean clearTransactionId,
            @JsonProperty("currentHeartBeat") DateTime currentHeartBeat)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.queryState = requireNonNull(queryState, "queryState is null");
        this.startedTransactionId = requireNonNull(startedTransactionId, "startedTransactionId is null");
        this.clearTransactionId = clearTransactionId;
        this.currentHeartBeat = requireNonNull(currentHeartBeat, "currentHeartBeat is null");
    }

    public DispatchQueryInfo(QueryInfo queryInfo)
    {
        this(queryInfo.getQueryId(),
                queryInfo.getState(),
                queryInfo.getStartedTransactionId(),
                queryInfo.isClearTransactionId(),
                queryInfo.getQueryStats().getLastHeartbeat());
    }

    @JsonProperty
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public QueryState getQueryState()
    {
        return queryState;
    }

    @JsonProperty
    public Optional<TransactionId> getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @JsonProperty
    public boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    @JsonProperty
    public DateTime getCurrentHeartBeat()
    {
        return currentHeartBeat;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("queryState", queryState)
                .add("startedTransactionId", startedTransactionId.map(TransactionId::toString))
                .add("clearTransactionId", clearTransactionId)
                .add("currentHeartBeat", currentHeartBeat)
                .toString();
    }
}

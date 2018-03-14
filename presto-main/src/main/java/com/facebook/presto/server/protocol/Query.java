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
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.client.QueryError;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.PageTransportErrorException;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.ResponseHandlerUtils.propagate;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@ThreadSafe
class Query
{
    private static final String UNKNOWN_FIELD = "unknown";
    private static final Logger log = Logger.get(Query.class);
    private static final long DESIRED_RESULT_BYTES = new DataSize(1, MEGABYTE).toBytes();

    private final QueryManager queryManager;
    private final InternalNodeManager internalNodeManager;
    private final HttpClient httpClient;
    private final QueryId queryId;

    @GuardedBy("this")
    private final ExchangeClient exchangeClient;

    private final Executor resultsProcessorExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    @GuardedBy("this")
    private final PagesSerde serde;

    private final AtomicLong resultId = new AtomicLong();
    private final Session session;

    @GuardedBy("this")
    private QueryResults lastResult;

    @GuardedBy("this")
    private String lastResultPath;

    @GuardedBy("this")
    private List<Column> columns;

    @GuardedBy("this")
    private List<Type> types;

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

    @GuardedBy("this")
    private Long updateCount;

    public static Query create(
            SessionContext sessionContext,
            String query,
            QueryId queryId,
            InternalNodeManager internalNodeManager,
            QueryManager queryManager,
            HttpClient httpClient,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClient exchangeClient,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        Query result = new Query(
                sessionContext,
                query,
                queryId,
                internalNodeManager,
                queryManager,
                httpClient,
                sessionPropertyManager,
                exchangeClient,
                dataProcessorExecutor,
                timeoutExecutor, blockEncodingSerde);

        result.queryManager.addOutputInfoListener(result.getQueryId(), result::setQueryOutputInfo);

        result.queryManager.addStateChangeListener(result.getQueryId(), state -> {
            if (state.isDone()) {
                QueryInfo queryInfo = queryManager.getQueryInfo(result.getQueryId());
                result.closeExchangeClientIfNecessary(queryInfo);
            }
        });

        return result;
    }

    private Query(
            SessionContext sessionContext,
            String query,
            QueryId queryId,
            InternalNodeManager internalNodeManager,
            QueryManager queryManager,
            HttpClient httpClient,
            SessionPropertyManager sessionPropertyManager,
            ExchangeClient exchangeClient,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        requireNonNull(sessionContext, "sessionContext is null");
        requireNonNull(query, "query is null");
        requireNonNull(queryId, "queryId is null");
        requireNonNull(internalNodeManager, "internalNodeManager is null");
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(exchangeClient, "exchangeClient is null");
        requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        requireNonNull(timeoutExecutor, "timeoutExecutor is null");

        this.internalNodeManager = internalNodeManager;
        this.queryManager = queryManager;
        this.httpClient = httpClient;

        QueryInfo queryInfo = queryManager.createQuery(sessionContext, query, queryId, Optional.of(this::finishQueryIfNecessary));
        this.queryId = queryInfo.getQueryId();
        session = queryInfo.getSession().toSession(sessionPropertyManager);
        this.exchangeClient = exchangeClient;
        this.resultsProcessorExecutor = resultsProcessorExecutor;
        this.timeoutExecutor = timeoutExecutor;
        requireNonNull(blockEncodingSerde, "serde is null");
        this.serde = new PagesSerdeFactory(blockEncodingSerde, isExchangeCompressionEnabled(session)).createPagesSerde();
    }

    public void cancel()
    {
        queryManager.cancelQuery(queryId);
        dispose();
    }

    public synchronized void dispose()
    {
        exchangeClient.close();
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public synchronized Optional<String> getSetCatalog()
    {
        return setCatalog;
    }

    public synchronized Optional<String> getSetSchema()
    {
        return setSchema;
    }

    public synchronized Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    public synchronized Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    public synchronized Optional<TransactionId> getStartedTransactionId()
    {
        return startedTransactionId;
    }

    public synchronized boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    public synchronized ListenableFuture<QueryResults> waitForResults(OptionalLong token, UriInfo uriInfo, Duration wait)
    {
        // before waiting, check if this request has already been processed and cached
        if (token.isPresent()) {
            Optional<QueryResults> cachedResult = getCachedResult(token.getAsLong(), uriInfo);
            if (cachedResult.isPresent()) {
                return immediateFuture(cachedResult.get());
            }
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getNextResult(token, uriInfo), resultsProcessorExecutor);
    }

    // TODO: we have to use QueryResults because that is expected by the client for backward compatibility
    public synchronized ListenableFuture<QueryResults> waitForExecution(UriInfo uriInfo, Duration wait)
    {
        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getWaitResult(uriInfo), resultsProcessorExecutor);
    }

    private synchronized ListenableFuture<?> getFutureStateChange()
    {
        // if the exchange client is open, wait for data
        if (!exchangeClient.isClosed()) {
            return exchangeClient.isBlocked();
        }

        // otherwise, wait for the query to finish
        queryManager.recordHeartbeat(queryId);
        return queryManager.getQueryState(queryId).map(this::queryDoneFuture)
                .orElse(immediateFuture(null));
    }

    private synchronized Optional<QueryResults> getCachedResult(long token, UriInfo uriInfo)
    {
        // is the a repeated request for the last results?
        String requestedPath = uriInfo.getAbsolutePath().getPath();
        if (lastResultPath != null && requestedPath.equals(lastResultPath)) {
            // tell query manager we are still interested in the query
            queryManager.getQueryInfo(queryId);
            queryManager.recordHeartbeat(queryId);
            return Optional.of(lastResult);
        }

        if (token < resultId.get()) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        // if this is not a request for the next results, return not found
        if (lastResult.getNextUri() == null || !requestedPath.equals(lastResult.getNextUri().getPath())) {
            // unknown token
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return Optional.empty();
    }

    private synchronized QueryResults getNextResult(OptionalLong token, UriInfo uriInfo)
    {
        // check if the result for the token have already been created
        if (token.isPresent()) {
            Optional<QueryResults> cachedResult = getCachedResult(token.getAsLong(), uriInfo);
            if (cachedResult.isPresent()) {
                return cachedResult.get();
            }
        }

        // Remove as many pages as possible from the exchange until just greater than DESIRED_RESULT_BYTES
        // NOTE: it is critical that query results are created for the pages removed from the exchange
        // client while holding the lock because the query may transition to the finished state when the
        // last page is removed.  If another thread observes this state before the response is cached
        // the pages will be lost.
        Iterable<List<Object>> data = null;
        try {
            ImmutableList.Builder<RowIterable> pages = ImmutableList.builder();
            long bytes = 0;
            long rows = 0;
            while (bytes < DESIRED_RESULT_BYTES) {
                SerializedPage serializedPage = exchangeClient.pollPage();
                if (serializedPage == null) {
                    break;
                }

                Page page = serde.deserialize(serializedPage);
                bytes += page.getSizeInBytes();
                rows += page.getPositionCount();
                pages.add(new RowIterable(session.toConnectorSession(), types, page));
            }
            if (rows > 0) {
                // client implementations do not properly handle empty list of data
                data = Iterables.concat(pages.build());
            }
        }
        catch (Throwable cause) {
            queryManager.failQuery(queryId, cause);
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        // TODO: figure out a better way to do this
        // grab the update count for non-queries
        if ((data != null) && (queryInfo.getUpdateType() != null) && (updateCount == null) &&
                (columns.size() == 1) && (columns.get(0).getType().equals(StandardTypes.BIGINT))) {
            Iterator<List<Object>> iterator = data.iterator();
            if (iterator.hasNext()) {
                Number number = (Number) iterator.next().get(0);
                if (number != null) {
                    updateCount = number.longValue();
                }
            }
        }

        closeExchangeClientIfNecessary(queryInfo);

        // for queries with no output, return a fake result for clients that require it
        if ((queryInfo.getState() == QueryState.FINISHED) && !queryInfo.getOutputStage().isPresent()) {
            columns = ImmutableList.of(new Column("result", BooleanType.BOOLEAN));
            data = ImmutableSet.of(ImmutableList.of(true));
        }

        // only return a next if the query is not done or there is more data to send (due to buffering)
        URI nextResultsUri = null;
        if (!queryInfo.isFinalQueryInfo() || !exchangeClient.isClosed()) {
            nextResultsUri = createNextResultsUri(uriInfo);
        }

        updateInfo(queryInfo);

        // first time through, self is null
        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                findCancelableLeafStage(queryInfo),
                nextResultsUri,
                columns,
                data,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                queryInfo.getUpdateType(),
                updateCount);

        // cache the last results
        if (lastResult != null && lastResult.getNextUri() != null) {
            lastResultPath = lastResult.getNextUri().getPath();
        }
        else {
            lastResultPath = null;
        }
        lastResult = queryResults;
        return queryResults;
    }

    private synchronized QueryResults getWaitResult(UriInfo uriInfo)
    {
        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        closeExchangeClientIfNecessary(queryInfo);

        URI nextUri;
        if (queryInfo.getState() == QueryState.STARTING) {
            // createQuery end point now should take a query ID
            Set<Node> coordinators = internalNodeManager.getCoordinators();
            checkState(coordinators.size() == 1);
            nextUri = uriBuilderFrom(coordinators.iterator().next().getHttpUri()).replacePath("/v1/statement").appendPath(queryId.toString()).build();
            log.info("query " + queryId.toString() + " will be redirected to " + nextUri.toString());

            QueryResults queryResults = httpClient.execute(buildQueryRequest(nextUri, queryInfo.getSession(), queryInfo.getQuery()), new QueryResultsResponseHandler());
            nextUri = queryResults.getNextUri();

            if (nextUri == null) {
                queryManager.failQuery(queryId, new PrestoException(GENERIC_INTERNAL_ERROR, "empty next uri"));
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "empty next uri");
            }
            log.info("query " + queryId.toString() + " has been redirected; ack client with  " + nextUri.toString());
            queryManager.submitQuery(queryId);
        }
        else {
            nextUri = uriInfo.getBaseUriBuilder().replacePath("/v1/statement/wait").path(queryId.toString()).replaceQuery("").build();
            log.info("query " + queryId.toString() + " is still waiting at " + nextUri.toString());
        }

        updateInfo(queryInfo);

        // first time through, self is null
        return new QueryResults(
                queryId.toString(),
                uriInfo.getRequestUriBuilder().replaceQuery(queryId.toString()).replacePath("query.html").build(),
                null,
                nextUri,
                ImmutableList.of(),
                null,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                queryInfo.getUpdateType(),
                updateCount);
    }

    private Request buildQueryRequest(URI uri, SessionRepresentation session, String query)
    {
        Request.Builder builder = preparePost()
                .addHeader(PRESTO_USER, session.getUser())
                .addHeader(USER_AGENT, session.getUserAgent().orElse(UNKNOWN_FIELD))
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8));

        if (session.getSource() != null) {
            builder.addHeader(PRESTO_SOURCE, session.getSource().orElse(UNKNOWN_FIELD));
        }
        if (session.getClientTags() != null && !session.getClientTags().isEmpty()) {
            builder.addHeader(PRESTO_CLIENT_TAGS, Joiner.on(",").join(session.getClientTags()));
        }
        if (session.getClientInfo() != null) {
            builder.addHeader(PRESTO_CLIENT_INFO, session.getClientInfo().orElse(UNKNOWN_FIELD));
        }
        if (session.getCatalog() != null) {
            builder.addHeader(PRESTO_CATALOG, session.getCatalog().orElse(UNKNOWN_FIELD));
        }
        if (session.getSchema() != null) {
            builder.addHeader(PRESTO_SCHEMA, session.getSchema().orElse(UNKNOWN_FIELD));
        }
        builder.addHeader(PRESTO_TIME_ZONE, session.getTimeZoneKey().getId());
        if (session.getLocale() != null) {
            builder.addHeader(PRESTO_LANGUAGE, session.getLocale().toLanguageTag());
        }

        Map<String, String> property = session.getSystemProperties();
        for (Map.Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PRESTO_SESSION, entry.getKey() + "=" + entry.getValue());
        }

        Map<String, String> statements = session.getPreparedStatements();
        for (Map.Entry<String, String> entry : statements.entrySet()) {
            builder.addHeader(PRESTO_PREPARED_STATEMENT, urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
        }

        //builder.addHeader(PRESTO_TRANSACTION_ID,  session.getTransactionId().map(TransactionId::toString).orElse("NONE"));
        builder.addHeader(PRESTO_TRANSACTION_ID,  "NONE");

        return builder.build();
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    private void updateInfo(QueryInfo queryInfo)
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

    private synchronized void closeExchangeClientIfNecessary(QueryInfo queryInfo)
    {
        // Close the exchange client if the query has failed, or if the query
        // is done and it does not have an output stage. The latter happens
        // for data definition executions, as those do not have output.
        if ((queryInfo.getState() == QueryState.FAILED) ||
                (queryInfo.getState().isDone() && !queryInfo.getOutputStage().isPresent())) {
            exchangeClient.close();
        }
    }

    private synchronized void finishQueryIfNecessary()
    {
        Set<Node> dispatchers = internalNodeManager.getDispatchers();
        checkState(dispatchers.size() == 1);
        URI uri = uriBuilderFrom(dispatchers.iterator().next().getHttpUri()).replacePath("/v1/statement").appendPath(queryId.toString()).build();
        httpClient.executeAsync(prepareDelete().setUri(uri).build(), new ResponseHandler<Void, RuntimeException>()
        {
            @Override
            public Void handleException(Request request, Exception exception)
            {
                log.debug(exception, "Acknowledge request failed: %s", uri);
                return null;
            }

            @Override
            public Void handle(Request request, io.airlift.http.client.Response response)
            {
                if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                    log.debug("Unexpected acknowledge response code: %s", response.getStatusCode());
                }
                return null;
            }
        });
        log.info("sent cancel query " + queryId.toString());
    }

    private synchronized void setQueryOutputInfo(QueryExecution.QueryOutputInfo outputInfo)
    {
        // if first callback, set column names
        if (columns == null) {
            List<String> columnNames = outputInfo.getColumnNames();
            List<Type> columnTypes = outputInfo.getColumnTypes();
            checkArgument(columnNames.size() == columnTypes.size(), "Column names and types size mismatch");

            ImmutableList.Builder<Column> list = ImmutableList.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                list.add(new Column(columnNames.get(i), columnTypes.get(i)));
            }
            columns = list.build();
            types = outputInfo.getColumnTypes();
        }

        for (URI outputLocation : outputInfo.getBufferLocations()) {
            exchangeClient.addLocation(outputLocation);
        }
        if (outputInfo.isNoMoreBufferLocations()) {
            exchangeClient.noMoreLocations();
        }
    }

    private ListenableFuture<?> queryDoneFuture(QueryState currentState)
    {
        if (currentState.isDone()) {
            return immediateFuture(null);
        }
        return Futures.transformAsync(queryManager.getStateChange(queryId, currentState), this::queryDoneFuture);
    }

    private synchronized URI createNextResultsUri(UriInfo uriInfo)
    {
        return uriInfo.getBaseUriBuilder().replacePath("/v1/statement").path(queryId.toString()).path(String.valueOf(resultId.incrementAndGet())).replaceQuery("").build();
    }

    private static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes(outputStage).size())
                .setTotalSplits(queryStats.getTotalDrivers())
                .setQueuedSplits(queryStats.getQueuedDrivers())
                .setRunningSplits(queryStats.getRunningDrivers() + queryStats.getBlockedDrivers())
                .setCompletedSplits(queryStats.getCompletedDrivers())
                .setUserTimeMillis(queryStats.getTotalUserTime().toMillis())
                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .setProcessedRows(queryStats.getRawInputPositions())
                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                .setPeakMemoryBytes(queryStats.getPeakUserMemoryReservation().toBytes())
                .setRootStage(toStageStats(outputStage))
                .build();
    }

    private static StageStats toStageStats(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return null;
        }

        com.facebook.presto.execution.StageStats stageStats = stageInfo.getStageStats();

        ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
        for (StageInfo subStage : stageInfo.getSubStages()) {
            subStages.add(toStageStats(subStage));
        }

        Set<String> uniqueNodes = new HashSet<>();
        for (TaskInfo task : stageInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
        }

        return StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(stageInfo.getState().toString())
                .setDone(stageInfo.getState().isDone())
                .setNodes(uniqueNodes.size())
                .setTotalSplits(stageStats.getTotalDrivers())
                .setQueuedSplits(stageStats.getQueuedDrivers())
                .setRunningSplits(stageStats.getRunningDrivers() + stageStats.getBlockedDrivers())
                .setCompletedSplits(stageStats.getCompletedDrivers())
                .setUserTimeMillis(stageStats.getTotalUserTime().toMillis())
                .setCpuTimeMillis(stageStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageStats.getRawInputPositions())
                .setProcessedBytes(stageStats.getRawInputDataSize().toBytes())
                .setSubStages(subStages.build())
                .build();
    }

    private static Set<String> globalUniqueNodes(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
        for (TaskInfo task : stageInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            nodes.add(uri.getHost() + ":" + uri.getPort());
        }

        for (StageInfo subStage : stageInfo.getSubStages()) {
            nodes.addAll(globalUniqueNodes(subStage));
        }
        return nodes.build();
    }

    private static URI findCancelableLeafStage(QueryInfo queryInfo)
    {
        // if query is running, find the leaf-most running stage
        return queryInfo.getOutputStage().map(Query::findCancelableLeafStage).orElse(null);
    }

    private static URI findCancelableLeafStage(StageInfo stage)
    {
        // if this stage is already done, we can't cancel it
        if (stage.getState().isDone()) {
            return null;
        }

        // attempt to find a cancelable sub stage
        // check in reverse order since build side of a join will be later in the list
        for (StageInfo subStage : Lists.reverse(stage.getSubStages())) {
            URI leafStage = findCancelableLeafStage(subStage);
            if (leafStage != null) {
                return leafStage;
            }
        }

        // no matching sub stage, so return this stage
        return stage.getSelf();
    }

    private static QueryError toQueryError(QueryInfo queryInfo)
    {
        FailureInfo failure = queryInfo.getFailureInfo();
        if (failure == null) {
            QueryState state = queryInfo.getState();
            if ((!state.isDone()) || (state == QueryState.FINISHED)) {
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

    private static class QueryResultsResponseHandler
            implements ResponseHandler<QueryResults, RuntimeException>
    {
        private static final JsonCodec<QueryResults> CODEC = JsonCodec.jsonCodec(QueryResults.class);
        private final FullJsonResponseHandler<QueryResults> handler;

        public QueryResultsResponseHandler()
        {
            handler = createFullJsonResponseHandler(CODEC);
        }

        @Override
        public QueryResults handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public QueryResults handle(Request request, io.airlift.http.client.Response response)
        {
            FullJsonResponseHandler.JsonResponse<QueryResults> jsonResponse;
            try {
                jsonResponse = handler.handle(request, response);
            }
            catch (RuntimeException e) {
                throw new PageTransportErrorException(format("Error fetching %s: %s", request.getUri().toASCIIString(), e.getMessage()), e);
            }

            if (response.getStatusCode() != HttpStatus.OK.code()) {
                throw new PageTransportErrorException(format(
                        "Expected response code to be 200, but was %s %s:%n%s",
                        response.getStatusCode(),
                        response.getStatusMessage(),
                        response.toString()));
            }

            // invalid content type can happen when an error page is returned, but is unlikely given the above 200
            String contentType = response.getHeader(CONTENT_TYPE);
            if (contentType == null) {
                throw new PageTransportErrorException(format("%s header is not set: %s", CONTENT_TYPE, response));
            }
            if (!contentType.equals(APPLICATION_JSON)) {
                throw new PageTransportErrorException(format("Expected %s response from server but got %s", APPLICATION_JSON, contentType));
            }

            return jsonResponse.getValue();
        }
    }
}

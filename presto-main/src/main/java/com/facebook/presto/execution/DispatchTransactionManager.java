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

import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class DispatchTransactionManager
{
    private static final JsonCodec<List<TransactionId>> TRANSACTION_ID_LIST_CODEC = listJsonCodec(TransactionId.class);
    private static final Logger log = Logger.get(DispatchTransactionManager.class);

    private final ScheduledExecutorService scheduler;
    private final HttpClient httpClient;

    private final Map<TransactionId, URI> transactions = new ConcurrentHashMap<>();

    @Inject
    public DispatchTransactionManager(@ForDispatchTransaction HttpClient httpClient)
    {
        this.scheduler = newSingleThreadScheduledExecutor(threadsNamed("dispatch-manager-scheduler"));
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @PostConstruct
    public void start()
    {
        scheduler.scheduleWithFixedDelay(this::synchronizeTransaction, 0, 2, SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        scheduler.shutdownNow();
    }

    public void addTransaction(TransactionId transactionId, URI uri)
    {
        requireNonNull(transactionId, "transactionId is null");
        requireNonNull(uri, "uri is null");
        transactions.put(transactionId, uri);
    }

    public void removeTransaction(TransactionId transactionId)
    {
        transactions.remove(requireNonNull(transactionId, "transactionId is null"));
    }

    @Nullable
    public URI getCoordinator(TransactionId transactionId)
    {
        return transactions.get(requireNonNull(transactionId, "transactionId is null"));
    }

    /**
     * Fetch live transactions from all coordinators to remove expired transactions on the dispatcher
     */
    private void synchronizeTransaction()
    {
        try {
            Multimap<URI, TransactionId> coordinators = Multimaps.invertFrom(Multimaps.forMap(transactions), ArrayListMultimap.create());

            for (URI coordinator : coordinators.keySet()) {
                try {
                    URI uri = UriBuilder.fromUri(coordinator).replacePath("v1/transaction").build();
                    httpClient.execute(prepareGet().setUri(uri).build(), new ResponseHandler<Void, RuntimeException>()
                    {
                        @Override
                        public Void handleException(Request request, Exception exception)
                        {
                            log.debug(exception, "failed to fetch transaction info from %s", uri);
                            return null;
                        }

                        @Override
                        public Void handle(Request request, Response response)
                        {
                            if (response.getStatusCode() != HttpStatus.OK.code()) {
                                log.debug("Unexpected fetching transaction response code: %s", response.getStatusCode());
                                return null;
                            }
                            // use Set instead of List to avoid using removeAll
                            Set<TransactionId> liveTransactionIds = ImmutableSet.copyOf(createFullJsonResponseHandler(TRANSACTION_ID_LIST_CODEC).handle(request, response).getValue());
                            for (TransactionId transactionId : coordinators.get(coordinator)) {
                                if (liveTransactionIds.contains(transactionId)) {
                                    continue;
                                }
                                transactions.remove(transactionId);
                                log.debug("remove staled transaction %", transactionId);
                            }
                            return null;
                        }
                    });
                }
                catch (Exception e) {
                    log.debug(e, "synchronizing transaction failed");
                }
            }
        }
        catch (Exception e) {
            log.debug(e, "synchronizing transaction failed");
        }
    }
}

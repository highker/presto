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
package com.facebook.presto.operator;

import com.facebook.presto.execution.SystemMemoryUsageListener;
import com.facebook.presto.execution.buffer.BufferSummary;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

public class ExchangeClientFactory
        implements ExchangeClientSupplier
{
    private final DataSize maxBufferedBytes;
    private final Duration minErrorDuration;
    private final Duration maxErrorDuration;
    private final HttpClient httpClient;
    private final DataSize maxResponseSize;
    private final ScheduledExecutorService executor;
    private final JsonCodec<BufferSummary> bufferSummaryCodec;

    @Inject
    public ExchangeClientFactory(
            ExchangeClientConfig config,
            @ForExchange HttpClient httpClient,
            @ForExchange ScheduledExecutorService executor,
            JsonCodec<BufferSummary> bufferSummaryCodec)
    {
        this(
                config.getMaxBufferSize(),
                config.getMaxResponseSize(),
                config.getMinErrorDuration(),
                config.getMaxErrorDuration(),
                httpClient,
                executor,
                bufferSummaryCodec);
    }

    public ExchangeClientFactory(
            DataSize maxBufferedBytes,
            DataSize maxResponseSize,
            Duration minErrorDuration,
            Duration maxErrorDuration,
            HttpClient httpClient,
            ScheduledExecutorService executor,
            JsonCodec<BufferSummary> bufferSummaryCodec)
    {
        this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");
        this.minErrorDuration = requireNonNull(minErrorDuration, "minErrorDuration is null");
        this.maxErrorDuration = requireNonNull(maxErrorDuration, "maxErrorDuration is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        // Use only 0.75 of the maxResponseSize to leave room for additional bytes from the encoding
        // TODO figure out a better way to compute the size of data that will be transferred over the network
        requireNonNull(maxResponseSize, "maxResponseSize is null");
        long maxResponseSizeBytes = (long) (Math.min(httpClient.getMaxContentLength(), maxResponseSize.toBytes()) * 0.75);
        this.maxResponseSize = new DataSize(maxResponseSizeBytes, BYTE);

        this.executor = requireNonNull(executor, "executor is null");
        this.bufferSummaryCodec = requireNonNull(bufferSummaryCodec, "bufferSummaryCodec is null");

        checkArgument(maxBufferedBytes.toBytes() > 0, "maxBufferSummary must be at least 1 byte: %s", maxBufferedBytes);
        checkArgument(maxResponseSize.toBytes() > 0, "maxResponseSize must be at least 1 byte: %s", maxResponseSize);
    }

    @Override
    public ExchangeClient get(SystemMemoryUsageListener systemMemoryUsageListener)
    {
        return new ExchangeClient(
                maxBufferedBytes,
                maxResponseSize,
                minErrorDuration,
                maxErrorDuration,
                httpClient,
                executor,
                systemMemoryUsageListener,
                bufferSummaryCodec);
    }
}

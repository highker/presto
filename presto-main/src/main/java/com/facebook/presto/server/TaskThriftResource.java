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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThriftService(value = "task", idlName = "TaskThriftResource")
public class TaskThriftResource
{
    private static final Duration DEFAULT_MAX_WAIT_TIME = new Duration(20, SECONDS);

    private final TaskManager taskManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    @Inject
    public TaskThriftResource(
            TaskManager taskManager,
            @ForAsyncHttp BoundedExecutor responseExecutor,
            @ForAsyncHttp ScheduledExecutorService timeoutExecutor)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @ThriftMethod
    public ListenableFuture<BufferResult> getResults(TaskId taskId, OutputBufferId bufferId, long token, long maxSizeInBytes)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        ListenableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, bufferId, token, new DataSize(maxSizeInBytes, BYTE));
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        return addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(taskManager.getTaskInstanceId(taskId), token, false),
                waitTime,
                timeoutExecutor);
    }

    private static Duration randomizeWaitTime(Duration waitTime)
    {
        // Randomize in [T/2, T], so wait is not near zero and the client-supplied max wait time is respected
        long halfWaitMillis = waitTime.toMillis() / 2;
        return new Duration(halfWaitMillis + ThreadLocalRandom.current().nextLong(halfWaitMillis), MILLISECONDS);
    }
}

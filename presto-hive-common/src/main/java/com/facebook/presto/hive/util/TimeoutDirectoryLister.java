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
package com.facebook.presto.hive.util;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.concurrent.ExecutorServiceAdapter;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.NestedDirectoryPolicy;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import javax.annotation.PreDestroy;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_DIRECTORY_LISTING_TIMEOUT;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TimeoutDirectoryLister
        implements DirectoryLister
{
    private final ExecutorService executor;
    private final DirectoryLister directoryLister;
    private final SimpleTimeLimiter limiter;
    private final Duration timeout;

    public TimeoutDirectoryLister(DirectoryLister directoryLister, String connectorId, Duration timeout, int maxThreads)
    {
        requireNonNull(directoryLister, "directoryLister is null");
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(timeout, "timeout is null");

        this.executor = newCachedThreadPool(daemonThreadsNamed("hive-" + connectorId + "-directory-lister-%s"));
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        ExecutorService bounded = new ExecutorServiceAdapter(new BoundedExecutor(executor, maxThreads));
        this.limiter = SimpleTimeLimiter.create(bounded);
        this.timeout = requireNonNull(timeout, "timeout is null");
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Override
    public Iterator<HiveFileInfo> list(ExtendedFileSystem fs, Table table, Path path, NamenodeStats namenodeStats, NestedDirectoryPolicy nestedDirectoryPolicy, PathFilter pathFilter)
    {
        return new AbstractIterator<HiveFileInfo>()
        {
            Iterator<HiveFileInfo> list = directoryLister.list(fs, table, path, namenodeStats, nestedDirectoryPolicy, pathFilter);

            @Override
            protected HiveFileInfo computeNext()
            {
                try {
                    return limiter.callWithTimeout(() -> {
                        if (!list.hasNext()) {
                            return endOfData();
                        }
                        return list.next();
                    }, timeout.toMillis(), MILLISECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                catch (ExecutionException | UncheckedExecutionException e) {
                    throwIfInstanceOf(e.getCause(), PrestoException.class);
                    throw new RuntimeException(e);
                }
                catch (TimeoutException e) {
                    throw new PrestoException(HIVE_DIRECTORY_LISTING_TIMEOUT, "Listing directory timed out: " + path);
                }
            }
        };
    }
}

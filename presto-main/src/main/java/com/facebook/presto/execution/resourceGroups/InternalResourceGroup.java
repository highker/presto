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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.resourceGroups.WeightedFairQueue.Usage;
import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.ResourceGroupStateInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.resourceGroups.ResourceGroup;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupState;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.facebook.presto.SystemSessionProperties.getQueryPriority;
import static com.facebook.presto.server.QueryStateInfo.createQueryStateInfo;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_QUEUE;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_RUN;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.FULL;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.FAIR;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.QUERY_PRIORITY;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED;
import static com.facebook.presto.spi.resourceGroups.SchedulingPolicy.WEIGHTED_FAIR;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Resource groups form a tree, and all access to a group is guarded by the root of the tree.
 * Queries are submitted to leaf groups. Never to intermediate groups. Intermediate groups
 * aggregate resource consumption from their children, and may have their own limitations that
 * are enforced.
 */
@ThreadSafe
public class InternalResourceGroup
        implements ResourceGroup
{
    public static final int DEFAULT_WEIGHT = 1;

    private final InternalResourceGroup root;
    private final Optional<InternalResourceGroup> parent;
    private final ResourceGroupId id;
    private final BiConsumer<InternalResourceGroup, Boolean> jmxExportListener;
    private final Executor executor;

    private final Map<String, InternalResourceGroup> subGroups = new ConcurrentHashMap<>();
    // Sub groups with queued queries, that have capacity to run them
    // That is, they must return true when internalStartNext() is called on them
    private Queue<InternalResourceGroup> eligibleSubGroups = new FifoQueue<>();
    // Sub groups whose memory usage may be out of date. Most likely because they have a running query.
    private final Set<InternalResourceGroup> dirtySubGroups = ConcurrentHashMap.newKeySet();
    private AtomicLong softMemoryLimitBytes = new AtomicLong();
    private AtomicInteger softConcurrencyLimit = new AtomicInteger();
    private AtomicInteger hardConcurrencyLimit = new AtomicInteger();
    private AtomicInteger maxQueuedQueries = new AtomicInteger();
    private AtomicLong softCpuLimitMillis = new AtomicLong(Long.MAX_VALUE);
    private AtomicLong hardCpuLimitMillis = new AtomicLong(Long.MAX_VALUE);
    private AtomicLong cpuUsageMillis = new AtomicLong();
    private AtomicLong cpuQuotaGenerationMillisPerSecond = new AtomicLong(Long.MAX_VALUE);
    private AtomicInteger descendantRunningQueries = new AtomicInteger();
    private AtomicInteger descendantQueuedQueries = new AtomicInteger();
    // Memory usage is cached because it changes very rapidly while queries are running, and would be expensive to track continuously
    private AtomicLong cachedMemoryUsageBytes = new AtomicLong();
    private AtomicInteger schedulingWeight = new AtomicInteger(DEFAULT_WEIGHT);
    private UpdateablePriorityQueue<QueryExecution> queuedQueries = new FifoQueue<>();
    private final Set<QueryExecution> runningQueries = ConcurrentHashMap.newKeySet();
    private AtomicReference<SchedulingPolicy> schedulingPolicy = new AtomicReference<>(FAIR);
    private AtomicBoolean jmxExport = new AtomicBoolean();
    private AtomicReference<Duration> queuedTimeLimit = new AtomicReference<>(new Duration(Long.MAX_VALUE, MILLISECONDS));
    private AtomicReference<Duration> runningTimeLimit = new AtomicReference<>(new Duration(Long.MAX_VALUE, MILLISECONDS));

    protected InternalResourceGroup(Optional<InternalResourceGroup> parent, String name, BiConsumer<InternalResourceGroup, Boolean> jmxExportListener, Executor executor)
    {
        this.parent = requireNonNull(parent, "parent is null");
        this.jmxExportListener = requireNonNull(jmxExportListener, "jmxExportListener is null");
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(name, "name is null");
        if (parent.isPresent()) {
            id = new ResourceGroupId(parent.get().id, name);
            root = parent.get().root;
        }
        else {
            id = new ResourceGroupId(name);
            root = this;
        }
    }

    public ResourceGroupInfo getInfo()
    {
        checkState(!subGroups.isEmpty() || (descendantRunningQueries.get() == 0 && descendantQueuedQueries.get() == 0), "Leaf resource group has descendant queries.");

        List<ResourceGroupInfo> infos = subGroups.values().stream()
                .map(InternalResourceGroup::getInfo)
                .collect(toImmutableList());

        return new ResourceGroupInfo(
                id,
                DataSize.succinctBytes(softMemoryLimitBytes.get()),
                hardConcurrencyLimit.get(),
                softConcurrencyLimit.get(),
                runningTimeLimit.get(),
                maxQueuedQueries.get(),
                queuedTimeLimit.get(),
                getState(),
                eligibleSubGroups.size(),
                DataSize.succinctBytes(cachedMemoryUsageBytes.get()),
                runningQueries.size() + descendantRunningQueries.get(),
                queuedQueries.size() + descendantQueuedQueries.get(),
                infos);
    }

    public ResourceGroupStateInfo getStateInfo()
    {
        return new ResourceGroupStateInfo(
                id,
                getState(),
                DataSize.succinctBytes(softMemoryLimitBytes.get()),
                DataSize.succinctBytes(cachedMemoryUsageBytes.get()),
                softConcurrencyLimit.get(),
                hardConcurrencyLimit.get(),
                maxQueuedQueries.get(),
                runningTimeLimit.get(),
                queuedTimeLimit.get(),
                getAggregatedRunningQueriesInfo(),
                queuedQueries.size() + descendantQueuedQueries.get(),
                subGroups.values().stream()
                        .map(subGroup -> new ResourceGroupInfo(
                                subGroup.getId(),
                                DataSize.succinctBytes(subGroup.softMemoryLimitBytes.get()),
                                subGroup.softConcurrencyLimit.get(),
                                subGroup.hardConcurrencyLimit.get(),
                                subGroup.runningTimeLimit.get(),
                                subGroup.maxQueuedQueries.get(),
                                subGroup.queuedTimeLimit.get(),
                                subGroup.getState(),
                                subGroup.eligibleSubGroups.size(),
                                DataSize.succinctBytes(subGroup.cachedMemoryUsageBytes.get()),
                                subGroup.runningQueries.size() + subGroup.descendantRunningQueries.get(),
                                subGroup.queuedQueries.size() + subGroup.descendantQueuedQueries.get()))
                        .collect(toImmutableList()));
    }

    private ResourceGroupState getState()
    {
        synchronized (root) {
            if (canRunMore()) {
                return CAN_RUN;
            }
            else if (canQueueMore()) {
                return CAN_QUEUE;
            }
            else {
                return FULL;
            }
        }
    }

    private List<QueryStateInfo> getAggregatedRunningQueriesInfo()
    {
        synchronized (root) {
            if (subGroups.isEmpty()) {
                return runningQueries.stream()
                        .map(QueryExecution::getQueryInfo)
                        .map(queryInfo -> createQueryStateInfo(queryInfo, Optional.of(id), Optional.empty()))
                        .collect(toImmutableList());
            }
            return subGroups.values().stream()
                    .map(InternalResourceGroup::getAggregatedRunningQueriesInfo)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }
    }

    @Override
    public ResourceGroupId getId()
    {
        return id;
    }

    @Managed
    public int getRunningQueries()
    {
        return runningQueries.size() + descendantRunningQueries.get();
    }

    @Managed
    public int getQueuedQueries()
    {
        return queuedQueries.size() + descendantQueuedQueries.get();
    }

    @Managed
    public int getWaitingQueuedQueries()
    {
        // For leaf group, when no queries can run, all queued queries are waiting for resources on this resource group.
        if (subGroups.isEmpty()) {
            return queuedQueries.size();
        }

        synchronized (root) {
            // For internal groups, when no queries can run, only queries that could run on its subgroups are waiting for resources on this group.
            int waitingQueuedQueries = 0;
            for (InternalResourceGroup subGroup : subGroups.values()) {
                if (subGroup.canRunMore()) {
                    waitingQueuedQueries += min(subGroup.getQueuedQueries(), subGroup.getHardConcurrencyLimit() - subGroup.getRunningQueries());
                }
            }

            return waitingQueuedQueries;
        }
    }

    @Override
    public DataSize getSoftMemoryLimit()
    {
        return new DataSize(softMemoryLimitBytes.get(), BYTE);
    }

    @Override
    public void setSoftMemoryLimit(DataSize limit)
    {
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.softMemoryLimitBytes.set(limit.toBytes());
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
    }

    @Override
    public Duration getSoftCpuLimit()
    {
        return new Duration(softCpuLimitMillis.get(), MILLISECONDS);
    }

    @Override
    public void setSoftCpuLimit(Duration limit)
    {
        synchronized (root) {
            if (limit.toMillis() > hardCpuLimitMillis.get()) {
                setHardCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.softCpuLimitMillis.set(limit.toMillis());
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
    }

    @Override
    public Duration getHardCpuLimit()
    {
        return new Duration(hardCpuLimitMillis.get(), MILLISECONDS);
    }

    @Override
    public void setHardCpuLimit(Duration limit)
    {
        synchronized (root) {
            if (limit.toMillis() < softCpuLimitMillis.get()) {
                setSoftCpuLimit(limit);
            }
            boolean oldCanRun = canRunMore();
            this.hardCpuLimitMillis.set(limit.toMillis());
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
    }

    @Override
    public long getCpuQuotaGenerationMillisPerSecond()
    {
        return cpuQuotaGenerationMillisPerSecond.get();
    }

    @Override
    public void setCpuQuotaGenerationMillisPerSecond(long rate)
    {
        checkArgument(rate > 0, "Cpu quota generation must be positive");
        synchronized (root) {
            cpuQuotaGenerationMillisPerSecond.set(rate);
        }
    }

    @Override
    public int getSoftConcurrencyLimit()
    {
        return softConcurrencyLimit.get();
    }

    @Override
    public void setSoftConcurrencyLimit(int softConcurrencyLimit)
    {
        checkArgument(softConcurrencyLimit >= 0, "softConcurrencyLimit is negative");
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.softConcurrencyLimit.set(softConcurrencyLimit);
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
    }

    @Managed
    @Override
    public int getHardConcurrencyLimit()
    {
        return hardConcurrencyLimit.get();
    }

    @Managed
    @Override
    public void setHardConcurrencyLimit(int hardConcurrencyLimit)
    {
        checkArgument(hardConcurrencyLimit >= 0, "hardConcurrencyLimit is negative");
        synchronized (root) {
            boolean oldCanRun = canRunMore();
            this.hardConcurrencyLimit.set(hardConcurrencyLimit);
            if (canRunMore() != oldCanRun) {
                updateEligiblility();
            }
        }
    }

    @Managed
    @Override
    public int getMaxQueuedQueries()
    {
        return maxQueuedQueries.get();
    }

    @Managed
    @Override
    public void setMaxQueuedQueries(int maxQueuedQueries)
    {
        checkArgument(maxQueuedQueries >= 0, "maxQueuedQueries is negative");
        synchronized (root) {
            this.maxQueuedQueries.set(maxQueuedQueries);
        }
    }

    @Override
    public int getSchedulingWeight()
    {
        return schedulingWeight.get();
    }

    @Override
    public void setSchedulingWeight(int weight)
    {
        checkArgument(weight > 0, "weight must be positive");
        synchronized (root) {
            this.schedulingWeight.set(weight);
            if (parent.isPresent() && parent.get().schedulingPolicy.get() == WEIGHTED && parent.get().eligibleSubGroups.contains(this)) {
                parent.get().addOrUpdateSubGroup(this);
            }
        }
    }

    @Override
    public SchedulingPolicy getSchedulingPolicy()
    {
        return schedulingPolicy.get();
    }

    @Override
    public void setSchedulingPolicy(SchedulingPolicy policy)
    {
        synchronized (root) {
            if (policy == schedulingPolicy.get()) {
                return;
            }

            if (parent.isPresent() && parent.get().schedulingPolicy.get() == QUERY_PRIORITY) {
                checkArgument(policy == QUERY_PRIORITY, "Parent of %s uses query priority scheduling, so %s must also", id, id);
            }

            // Switch to the appropriate queue implementation to implement the desired policy
            Queue<InternalResourceGroup> queue;
            UpdateablePriorityQueue<QueryExecution> queryQueue;
            switch (policy) {
                case FAIR:
                    queue = new FifoQueue<>();
                    queryQueue = new FifoQueue<>();
                    break;
                case WEIGHTED:
                    queue = new StochasticPriorityQueue<>();
                    queryQueue = new StochasticPriorityQueue<>();
                    break;
                case WEIGHTED_FAIR:
                    queue = new WeightedFairQueue<>();
                    queryQueue = new IndexedPriorityQueue<>();
                    break;
                case QUERY_PRIORITY:
                    // Sub groups must use query priority to ensure ordering
                    for (InternalResourceGroup group : subGroups.values()) {
                        group.setSchedulingPolicy(QUERY_PRIORITY);
                    }
                    queue = new IndexedPriorityQueue<>();
                    queryQueue = new IndexedPriorityQueue<>();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported scheduling policy: " + policy);
            }
            while (!eligibleSubGroups.isEmpty()) {
                InternalResourceGroup group = eligibleSubGroups.poll();
                addOrUpdateSubGroup(group);
            }
            eligibleSubGroups = queue;
            while (!queuedQueries.isEmpty()) {
                QueryExecution query = queuedQueries.poll();
                queryQueue.addOrUpdate(query, getQueryPriority(query.getSession()));
            }
            queuedQueries = queryQueue;
            schedulingPolicy.set(policy);
        }
    }

    @Override
    public boolean getJmxExport()
    {
        return jmxExport.get();
    }

    @Override
    public void setJmxExport(boolean export)
    {
        synchronized (root) {
            jmxExport.set(export);
        }
        jmxExportListener.accept(this, export);
    }

    @Override
    public Duration getQueuedTimeLimit()
    {
        return queuedTimeLimit.get();
    }

    @Override
    public void setQueuedTimeLimit(Duration queuedTimeLimit)
    {
        synchronized (root) {
            this.queuedTimeLimit.set(queuedTimeLimit);
        }
    }

    @Override
    public Duration getRunningTimeLimit()
    {
        return runningTimeLimit.get();
    }

    @Override
    public void setRunningTimeLimit(Duration runningTimeLimit)
    {
        synchronized (root) {
            this.runningTimeLimit.set(runningTimeLimit);
        }
    }

    public InternalResourceGroup getOrCreateSubGroup(String name)
    {
        requireNonNull(name, "name is null");
        synchronized (root) {
            checkArgument(runningQueries.isEmpty() && queuedQueries.isEmpty(), "Cannot add sub group to %s while queries are running", id);
            if (subGroups.containsKey(name)) {
                return subGroups.get(name);
            }
            InternalResourceGroup subGroup = new InternalResourceGroup(Optional.of(this), name, jmxExportListener, executor);
            // Sub group must use query priority to ensure ordering
            if (schedulingPolicy.get() == QUERY_PRIORITY) {
                subGroup.setSchedulingPolicy(QUERY_PRIORITY);
            }
            subGroups.put(name, subGroup);
            return subGroup;
        }
    }

    public void run(QueryExecution query)
    {
        synchronized (root) {
            checkState(subGroups.isEmpty(), "Cannot add queries to %s. It is not a leaf group.", id);
            // Check all ancestors for capacity
            query.setResourceGroup(id);
            InternalResourceGroup group = this;
            boolean canQueue = true;
            boolean canRun = true;
            while (true) {
                canQueue &= group.canQueueMore();
                canRun &= group.canRunMore();
                if (!group.parent.isPresent()) {
                    break;
                }
                group = group.parent.get();
            }
            if (!canQueue && !canRun) {
                query.fail(new QueryQueueFullException(id));
                return;
            }
            if (canRun) {
                startInBackground(query);
            }
            else {
                enqueueQuery(query);
            }
            query.addStateChangeListener(state -> {
                if (state.isDone()) {
                    queryFinished(query);
                }
            });
            if (query.getState().isDone()) {
                queryFinished(query);
            }
        }
    }

    private void enqueueQuery(QueryExecution query)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to enqueue a query");
        synchronized (root) {
            queuedQueries.addOrUpdate(query, getQueryPriority(query.getSession()));
            InternalResourceGroup group = this;
            while (group.parent.isPresent()) {
                group.parent.get().descendantQueuedQueries.incrementAndGet();
                group = group.parent.get();
            }
            updateEligiblility();
        }
    }

    // This method must be called whenever the group's eligibility to run more queries may have changed.
    private void updateEligiblility()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to update eligibility");
        synchronized (root) {
            if (!parent.isPresent()) {
                return;
            }
            if (isEligibleToStartNext()) {
                parent.get().addOrUpdateSubGroup(this);
            }
            else {
                parent.get().eligibleSubGroups.remove(this);
            }
            parent.get().updateEligiblility();
        }
    }

    private void startInBackground(QueryExecution query)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to start a query");
        synchronized (root) {
            runningQueries.add(query);
            InternalResourceGroup group = this;
            while (group.parent.isPresent()) {
                group.parent.get().descendantRunningQueries.incrementAndGet();
                group.parent.get().dirtySubGroups.add(group);
                group = group.parent.get();
            }
            updateEligiblility();
            executor.execute(query::start);
        }
    }

    private void queryFinished(QueryExecution query)
    {
        synchronized (root) {
            if (!runningQueries.contains(query) && !queuedQueries.contains(query)) {
                // Query has already been cleaned up
                return;
            }
            // Only count the CPU time if the query succeeded, or the failure was the fault of the user
            if (query.getState() == QueryState.FINISHED || query.getQueryInfo().getErrorType() == USER_ERROR) {
                InternalResourceGroup group = this;
                while (group != null) {
                    try {
                        group.cpuUsageMillis.set(Math.addExact(group.cpuUsageMillis.get(), query.getTotalCpuTime().toMillis()));
                    }
                    catch (ArithmeticException e) {
                        group.cpuUsageMillis.set(Long.MAX_VALUE);
                    }
                    group = group.parent.orElse(null);
                }
            }
            if (runningQueries.contains(query)) {
                runningQueries.remove(query);
                InternalResourceGroup group = this;
                while (group.parent.isPresent()) {
                    group.parent.get().descendantRunningQueries.decrementAndGet();
                    group = group.parent.get();
                }
            }
            else {
                queuedQueries.remove(query);
                InternalResourceGroup group = this;
                while (group.parent.isPresent()) {
                    group.parent.get().descendantQueuedQueries.decrementAndGet();
                    group = group.parent.get();
                }
            }
            updateEligiblility();
        }
    }

    // Memory usage stats are expensive to maintain, so this method must be called periodically to update them
    protected void internalRefreshStats()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to refresh stats");
        synchronized (root) {
            if (subGroups.isEmpty()) {
                cachedMemoryUsageBytes.set(0);
                for (QueryExecution query : runningQueries) {
                    cachedMemoryUsageBytes.getAndAdd(query.getTotalMemoryReservation());
                }
            }
            else {
                for (Iterator<InternalResourceGroup> iterator = dirtySubGroups.iterator(); iterator.hasNext(); ) {
                    InternalResourceGroup subGroup = iterator.next();
                    long oldMemoryUsageBytes = subGroup.cachedMemoryUsageBytes.get();
                    cachedMemoryUsageBytes.getAndAdd(-oldMemoryUsageBytes);
                    subGroup.internalRefreshStats();
                    cachedMemoryUsageBytes.getAndAdd(subGroup.cachedMemoryUsageBytes.get());
                    if (!subGroup.isDirty()) {
                        iterator.remove();
                    }
                    if (oldMemoryUsageBytes != subGroup.cachedMemoryUsageBytes.get()) {
                        subGroup.updateEligiblility();
                    }
                }
            }
        }
    }

    protected void internalGenerateCpuQuota(long elapsedSeconds)
    {
        checkState(Thread.holdsLock(root), "Must hold lock to generate cpu quota");
        synchronized (root) {
            long newQuota;
            try {
                newQuota = Math.multiplyExact(elapsedSeconds, cpuQuotaGenerationMillisPerSecond.get());
            }
            catch (ArithmeticException e) {
                newQuota = Long.MAX_VALUE;
            }
            try {
                cpuUsageMillis.set(Math.subtractExact(cpuUsageMillis.get(), newQuota));
            }
            catch (ArithmeticException e) {
                cpuUsageMillis.set(0);
            }
            cpuUsageMillis.set(Math.max(0, cpuUsageMillis.get()));
            for (InternalResourceGroup group : subGroups.values()) {
                group.internalGenerateCpuQuota(elapsedSeconds);
            }
        }
    }

    protected boolean internalStartNext()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to find next query");
        synchronized (root) {
            if (!canRunMore()) {
                return false;
            }
            QueryExecution query = queuedQueries.poll();
            if (query != null) {
                startInBackground(query);
                return true;
            }

            // Remove even if the sub group still has queued queries, so that it goes to the back of the queue
            InternalResourceGroup subGroup = eligibleSubGroups.poll();
            if (subGroup == null) {
                return false;
            }
            boolean started = subGroup.internalStartNext();
            checkState(started, "Eligible sub group had no queries to run");
            descendantQueuedQueries.decrementAndGet();
            // Don't call updateEligibility here, as we're in a recursive call, and don't want to repeatedly update our ancestors.
            if (subGroup.isEligibleToStartNext()) {
                addOrUpdateSubGroup(subGroup);
            }
            return true;
        }
    }

    protected void enforceTimeLimits()
    {
        checkState(Thread.holdsLock(root), "Must hold lock to enforce time limits");
        synchronized (root) {
            for (InternalResourceGroup group : subGroups.values()) {
                group.enforceTimeLimits();
            }
            for (QueryExecution query : runningQueries) {
                Duration runningTime = query.getQueryInfo().getQueryStats().getExecutionTime();
                if (runningQueries.contains(query) && runningTime != null && runningTime.compareTo(runningTimeLimit.get()) > 0) {
                    query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "query exceeded resource group runtime limit"));
                }
            }
            for (QueryExecution query : queuedQueries) {
                Duration elapsedTime = query.getQueryInfo().getQueryStats().getElapsedTime();
                if (queuedQueries.contains(query) && elapsedTime != null && elapsedTime.compareTo(queuedTimeLimit.get()) > 0) {
                    query.fail(new PrestoException(EXCEEDED_TIME_LIMIT, "query exceeded resource group queued time limit"));
                }
            }
        }
    }

    @GuardedBy("root")
    private void addOrUpdateSubGroup(InternalResourceGroup group)
    {
        if (schedulingPolicy.get() == WEIGHTED_FAIR) {
            ((WeightedFairQueue<InternalResourceGroup>) eligibleSubGroups).addOrUpdate(group, new Usage(group.getSchedulingWeight(), group.getRunningQueries()));
        }
        else {
            ((UpdateablePriorityQueue<InternalResourceGroup>) eligibleSubGroups).addOrUpdate(group, getSubGroupSchedulingPriority(schedulingPolicy.get(), group));
        }
    }

    @GuardedBy("root")
    private static long getSubGroupSchedulingPriority(SchedulingPolicy policy, InternalResourceGroup group)
    {
        if (policy == QUERY_PRIORITY) {
            return group.getHighestQueryPriority();
        }
        else {
            return group.computeSchedulingWeight();
        }
    }

    @GuardedBy("root")
    private long computeSchedulingWeight()
    {
        if (runningQueries.size() + descendantRunningQueries.get() >= softConcurrencyLimit.get()) {
            return schedulingWeight.get();
        }

        return (long) Integer.MAX_VALUE * schedulingWeight.get();
    }

    private boolean isDirty()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return runningQueries.size() + descendantRunningQueries.get() > 0;
        }
    }

    private boolean isEligibleToStartNext()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            if (!canRunMore()) {
                return false;
            }
            return !queuedQueries.isEmpty() || !eligibleSubGroups.isEmpty();
        }
    }

    private int getHighestQueryPriority()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            checkState(queuedQueries instanceof IndexedPriorityQueue, "Queued queries not ordered");
            if (queuedQueries.isEmpty()) {
                return 0;
            }
            return getQueryPriority(queuedQueries.peek().getSession());
        }
    }

    private boolean canQueueMore()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            return descendantQueuedQueries.get() + queuedQueries.size() < maxQueuedQueries.get();
        }
    }

    private boolean canRunMore()
    {
        checkState(Thread.holdsLock(root), "Must hold lock");
        synchronized (root) {
            if (cpuUsageMillis.get() >= hardCpuLimitMillis.get()) {
                return false;
            }

            int hardConcurrencyLimit = this.hardConcurrencyLimit.get();
            if (cpuUsageMillis.get() >= softCpuLimitMillis.get()) {
                // TODO: Consider whether cpu limit math should be performed on softConcurrency or hardConcurrency
                // Linear penalty between soft and hard limit
                double penalty = (cpuUsageMillis.get() - softCpuLimitMillis.get()) / (double) (hardCpuLimitMillis.get() - softCpuLimitMillis.get());
                hardConcurrencyLimit = (int) Math.floor(hardConcurrencyLimit * (1 - penalty));
                // Always penalize by at least one
                hardConcurrencyLimit = min(this.hardConcurrencyLimit.get() - 1, hardConcurrencyLimit);
                // Always allow at least one running query
                hardConcurrencyLimit = Math.max(1, hardConcurrencyLimit);
            }
            return runningQueries.size() + descendantRunningQueries.get() < hardConcurrencyLimit &&
                    cachedMemoryUsageBytes.get() <= softMemoryLimitBytes.get();
        }
    }

    public Collection<InternalResourceGroup> subGroups()
    {
        return subGroups.values();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof InternalResourceGroup)) {
            return false;
        }
        InternalResourceGroup that = (InternalResourceGroup) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @ThreadSafe
    public static final class RootInternalResourceGroup
            extends InternalResourceGroup
    {
        public RootInternalResourceGroup(String name, BiConsumer<InternalResourceGroup, Boolean> jmxExportListener, Executor executor)
        {
            super(Optional.empty(), name, jmxExportListener, executor);
        }

        public synchronized void processQueuedQueries()
        {
            internalRefreshStats();
            enforceTimeLimits();
            while (internalStartNext()) {
                // start all the queries we can
            }
        }

        public synchronized void generateCpuQuota(long elapsedSeconds)
        {
            if (elapsedSeconds > 0) {
                internalGenerateCpuQuota(elapsedSeconds);
            }
        }
    }
}

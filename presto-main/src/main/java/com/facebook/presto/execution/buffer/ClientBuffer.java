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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static com.facebook.presto.execution.buffer.BufferSummary.emptySummary;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class ClientBuffer
{
    private final String taskInstanceId;
    private final OutputBufferId bufferId;

    private final AtomicLong rowsAdded = new AtomicLong();
    private final AtomicLong pagesAdded = new AtomicLong();

    private final AtomicLong bufferedBytes = new AtomicLong();

    @GuardedBy("this")
    private final AtomicLong currentSequenceId = new AtomicLong();

    // the largest sequence id we have seen so far
    // no page with a larger sequence id is allowed to be fetched
    // this is for sanity check only
    @GuardedBy("this")
    private final AtomicLong currentMaxLookupId = new AtomicLong();

    @GuardedBy("this")
    private final LinkedList<SerializedPageReference> pages = new LinkedList<>();

    @GuardedBy("this")
    private boolean noMorePages;

    // destroyed is set when the client sends a DELETE to the buffer
    // this is an acknowledgement that the client has observed the end of the buffer
    @GuardedBy("this")
    private final AtomicBoolean destroyed = new AtomicBoolean();

    @GuardedBy("this")
    private PendingRead pendingRead;

    public ClientBuffer(String taskInstanceId, OutputBufferId bufferId)
    {
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.bufferId = requireNonNull(bufferId, "bufferId is null");
    }

    public BufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so state machine updates do not hang
        //

        @SuppressWarnings("FieldAccessNotGuarded")
        boolean destroyed = this.destroyed.get();

        @SuppressWarnings("FieldAccessNotGuarded")
        long sequenceId = this.currentSequenceId.get();

        // if destroyed the buffered page count must be zero regardless of observation ordering in this lock free code
        int bufferedPages = destroyed ? 0 : Math.max(toIntExact(pagesAdded.get() - sequenceId), 0);

        PageBufferInfo pageBufferInfo = new PageBufferInfo(bufferId.getId(), bufferedPages, bufferedBytes.get(), rowsAdded.get(), pagesAdded.get());
        return new BufferInfo(bufferId, destroyed, bufferedPages, sequenceId, pageBufferInfo);
    }

    public boolean isDestroyed()
    {
        //
        // NOTE: this code must be lock free so state machine updates do not hang
        //
        @SuppressWarnings("FieldAccessNotGuarded")
        boolean destroyed = this.destroyed.get();
        return destroyed;
    }

    public void destroy()
    {
        List<SerializedPageReference> removedPages;
        PendingRead pendingRead;
        synchronized (this) {
            removedPages = ImmutableList.copyOf(pages);
            pages.clear();

            bufferedBytes.getAndSet(0);

            noMorePages = true;
            destroyed.set(true);

            pendingRead = this.pendingRead;
            this.pendingRead = null;
        }

        removedPages.forEach(SerializedPageReference::dereferencePage);

        if (pendingRead != null) {
            pendingRead.completeBufferSummaryFutureWithEmpty();
        }
    }

    public void enqueuePages(Collection<SerializedPageReference> pages)
    {
        PendingRead pendingRead;
        synchronized (this) {
            // ignore pages after no more pages is set
            // this can happen with limit queries
            if (noMorePages) {
                return;
            }

            addPages(pages);

            pendingRead = this.pendingRead;
            this.pendingRead = null;
        }

        // we just added a page, so process the pending read
        if (pendingRead != null) {
            processRead(pendingRead);
        }
    }

    private synchronized void addPages(Collection<SerializedPageReference> pages)
    {
        pages.forEach(SerializedPageReference::addReference);
        this.pages.addAll(pages);

        long rowCount = pages.stream().mapToLong(SerializedPageReference::getPositionCount).sum();
        rowsAdded.addAndGet(rowCount);
        pagesAdded.addAndGet(pages.size());

        long bytesAdded = pages.stream().mapToLong(SerializedPageReference::getRetainedSizeInBytes).sum();
        bufferedBytes.addAndGet(bytesAdded);
    }

    public ListenableFuture<BufferSummary> getSummary(long sequenceId, long maxBytes)
    {
        return getSummary(sequenceId, maxBytes, Optional.empty());
    }

    public ListenableFuture<BufferSummary> getSummary(long sequenceId, long maxBytes, Optional<PagesSupplier> pagesSupplier)
    {
        checkArgument(sequenceId >= 0, "Invalid sequence id");

        // acknowledge pages first, out side of locks to not trigger callbacks while holding the lock
        acknowledgePages(sequenceId);

        // attempt to load some data before processing the read
        pagesSupplier.ifPresent(supplier -> loadPagesIfNecessary(supplier, maxBytes));

        PendingRead oldPendingRead = null;
        try {
            synchronized (this) {
                // save off the old pending read so we can abort it out side of the lock
                oldPendingRead = this.pendingRead;
                this.pendingRead = null;

                // Return results immediately if we have data, there will be no more data, or this is
                // an out of order request
                if (!pages.isEmpty() || noMorePages || sequenceId != currentSequenceId.get()) {
                    BufferSummary summary = processRead(sequenceId, maxBytes).summarize();
                    long nextSequenceId = sequenceId + summary.size();
                    long oldMaxLookupId = currentSequenceId.get();
                    while (nextSequenceId > oldMaxLookupId) {
                        currentMaxLookupId.compareAndSet(oldMaxLookupId, nextSequenceId);
                        oldMaxLookupId = currentMaxLookupId.get();
                    }
                    return immediateFuture(summary);
                }

                // otherwise, wait for more data to arrive
                pendingRead = new PendingRead(taskInstanceId, sequenceId, maxBytes);
                return pendingRead.getBufferSummaryFuture();
            }
        }
        finally {
            if (oldPendingRead != null) {
                // Each buffer is private to a single client, and each client should only have one outstanding
                // read.  Therefore, we abort the existing read since it was most likely abandoned by the client.
                oldPendingRead.completeBufferSummaryFutureWithEmpty();
            }
        }
    }

    public BufferResult getData(long sequenceId, long maxSizeInBytes)
    {
        checkArgument(sequenceId >= 0, "Invalid sequence id");
        checkArgument(sequenceId < currentMaxLookupId.get(), "Read more pages than peeked");

        // acknowledge pages first, out side of locks to not trigger callbacks while holding the lock
        acknowledgePages(sequenceId);

        synchronized (this) {
            // Must have data to call this method
            // otherwise it may be a out-of-order request or maybe the buffer has no more pages
            verify(!pages.isEmpty() || noMorePages || sequenceId != currentSequenceId.get());
            BufferResult result = processRead(sequenceId, maxSizeInBytes);
            checkArgument(sequenceId + result.size() <= currentMaxLookupId.get(), "Read more pages than peeked");
            return result;
        }
    }

    public void setNoMorePages()
    {
        PendingRead pendingRead;
        synchronized (this) {
            // ignore duplicate calls
            if (noMorePages) {
                return;
            }

            noMorePages = true;

            pendingRead = this.pendingRead;
            this.pendingRead = null;
        }

        // there will be no more pages, so process the pending read
        if (pendingRead != null) {
            processRead(pendingRead);
        }
    }

    public void loadPagesIfNecessary(PagesSupplier pagesSupplier)
    {
        requireNonNull(pagesSupplier, "pagesSupplier is null");

        // Get the max size from the current pending read, which may not be the
        // same pending read instance by the time pages are loaded but this is
        // safe since the size is rechecked before returning pages.
        long maxBytes;
        synchronized (this) {
            if (pendingRead == null) {
                return;
            }
            maxBytes = pendingRead.getMaxBytes();
        }

        boolean dataAdded = loadPagesIfNecessary(pagesSupplier, maxBytes);

        if (dataAdded) {
            PendingRead pendingRead;
            synchronized (this) {
                pendingRead = this.pendingRead;
            }
            if (pendingRead != null) {
                processRead(pendingRead);
            }
        }
    }

    /**
     * If there no data, attempt to load some from the pages supplier.
     */
    private boolean loadPagesIfNecessary(PagesSupplier pagesSupplier, long maxBytes)
    {
        checkState(!Thread.holdsLock(this), "Can not load pages while holding a lock on this");

        List<SerializedPageReference> pageReferences;
        synchronized (this) {
            if (noMorePages) {
                return false;
            }

            if (!pages.isEmpty()) {
                return false;
            }

            // The page supplier has incremented the page reference count, and addPages below also increments
            // the reference count, so we need to drop the page supplier reference. The call dereferencePage
            // is performed outside of synchronized to avoid making a callback while holding a lock.
            pageReferences = pagesSupplier.getPages(maxBytes);

            // add the pages to this buffer, which will increase the reference count
            addPages(pageReferences);

            // check for no more pages
            if (!pagesSupplier.mayHaveMorePages()) {
                noMorePages = true;
            }
        }

        // sent pages will have an initial reference count, so drop it
        pageReferences.forEach(SerializedPageReference::dereferencePage);

        return !pageReferences.isEmpty();
    }

    private void processRead(PendingRead pendingRead)
    {
        checkState(!Thread.holdsLock(this), "Can not process pending read while holding a lock on this");

        if (pendingRead.getBufferSummaryFuture().isDone()) {
            return;
        }

        long sequenceId = pendingRead.getSequenceId();
        BufferSummary summary = processRead(sequenceId, pendingRead.getMaxBytes()).summarize();

        long nextSequenceId = sequenceId + summary.size();
        long oldMaxLookupId = currentSequenceId.get();
        while (nextSequenceId > oldMaxLookupId) {
            currentMaxLookupId.compareAndSet(oldMaxLookupId, nextSequenceId);
            oldMaxLookupId = currentMaxLookupId.get();
        }
        pendingRead.getBufferSummaryFuture().set(summary);
    }

    /**
     * @return the page sizes of a result with at least one page if we have pages in buffer, empty result otherwise
     */
    private synchronized BufferResult processRead(long sequenceId, long maxBytes)
    {
        // When pages are added to the partition buffer they are effectively
        // assigned an id starting from zero. When a read is processed, the
        // "token" is the id of the page to start the read from, so the first
        // step of the read is to acknowledge, and drop all pages up to the
        // provided sequenceId.  Then pages starting from the sequenceId are
        // returned with the sequenceId of the next page to read.
        //
        // Since the buffer API is asynchronous there are a number of problems
        // that can occur our of order request (typically from retries due to
        // request failures):
        // - Request to read pages that have already been acknowledged.
        //   Simply, send an result with no pages and the requested sequenceId,
        //   and since the client has already acknowledge the pages, it will
        //   ignore the out of order response.
        // - Request to read after the buffer has been destroyed.  When the
        //   buffer is destroyed all pages are dropped, so the read sequenceId
        //   appears to be off the end of the queue.  Normally a read past the
        //   end of the queue would be be an error, but this specific case is
        //   detected and handled.  The client is sent an empty response with
        //   the finished flag set and next token is the max acknowledged page
        //   when the buffer is destroyed.
        //

        // if request is for pages before the current position, just return an empty result
        if (sequenceId < currentSequenceId.get()) {
            return emptyResults(taskInstanceId, sequenceId, false);
        }

        // if this buffer is finished, notify the client of this, so the client
        // will destroy this buffer
        if (pages.isEmpty() && noMorePages) {
            return emptyResults(taskInstanceId, currentSequenceId.get(), true);
        }

        // if request is for pages after the current position, there is a bug somewhere
        // a read call is always proceeded by acknowledge pages, which
        // will advance the sequence id to at least the request position, unless
        // the buffer is destroyed, and in that case the buffer will be empty with
        // no more pages set, which is checked above
        verify(sequenceId == currentSequenceId.get(), "Invalid sequence id");
        checkArgument(!pages.isEmpty(), "Fetch empty pages");

        // read the new pages
        List<SerializedPage> result = new ArrayList<>();
        long bytes = 0;

        for (SerializedPageReference page : pages) {
            bytes += page.getRetainedSizeInBytes();
            // break (and don't add) if this page would exceed the limit
            if (!result.isEmpty() && bytes > maxBytes) {
                break;
            }
            result.add(page.getSerializedPage());
        }
        return new BufferResult(taskInstanceId, sequenceId, sequenceId + result.size(), false, result);
    }

    /**
     * Drops pages up to the specified sequence id
     */
    private void acknowledgePages(long sequenceId)
    {
        checkState(!Thread.holdsLock(this), "Can not acknowledge pages while holding a lock on this");

        List<SerializedPageReference> removedPages = new ArrayList<>();
        synchronized (this) {
            if (destroyed.get()) {
                return;
            }

            // if pages have already been acknowledged, just ignore this
            long oldCurrentSequenceId = currentSequenceId.get();
            if (sequenceId < oldCurrentSequenceId) {
                return;
            }

            int pagesToRemove = toIntExact(sequenceId - oldCurrentSequenceId);
            checkArgument(pagesToRemove <= pages.size(), "Invalid sequence id");

            long bytesRemoved = 0;
            for (int i = 0; i < pagesToRemove; i++) {
                SerializedPageReference removedPage = pages.removeFirst();
                removedPages.add(removedPage);
                bytesRemoved += removedPage.getRetainedSizeInBytes();
            }

            // update current sequence id
            verify(currentSequenceId.compareAndSet(oldCurrentSequenceId, oldCurrentSequenceId + pagesToRemove));

            // update memory tracking
            verify(bufferedBytes.addAndGet(-bytesRemoved) >= 0);
        }

        // dereference outside of synchronized to avoid making a callback while holding a lock
        removedPages.forEach(SerializedPageReference::dereferencePage);
    }

    @Override
    public String toString()
    {
        @SuppressWarnings("FieldAccessNotGuarded")
        long sequenceId = currentSequenceId.get();

        @SuppressWarnings("FieldAccessNotGuarded")
        boolean destroyed = this.destroyed.get();

        return toStringHelper(this)
                .add("bufferId", bufferId)
                .add("sequenceId", sequenceId)
                .add("destroyed", destroyed)
                .toString();
    }

    @Immutable
    private static class PendingRead
    {
        private final String taskInstanceId;
        private final long sequenceId;
        private final long maxBytes;
        private final SettableFuture<BufferSummary> bufferSummaryFuture = SettableFuture.create();

        private PendingRead(String taskInstanceId, long sequenceId, long maxBytes)
        {
            this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
            this.sequenceId = sequenceId;
            this.maxBytes = maxBytes;
        }

        public long getSequenceId()
        {
            return sequenceId;
        }

        public long getMaxBytes()
        {
            return maxBytes;
        }

        public SettableFuture<BufferSummary> getBufferSummaryFuture()
        {
            return bufferSummaryFuture;
        }

        public void completeBufferSummaryFutureWithEmpty()
        {
            bufferSummaryFuture.set(emptySummary(taskInstanceId, sequenceId, false));
        }
    }

    public interface PagesSupplier
    {
        /**
         * Gets pages up to the specified size limit or a single page that exceeds the size limit.
         */
        List<SerializedPageReference> getPages(long maxBytes);

        /**
         * @return true if more pages may be produced; false otherwise
         */
        boolean mayHaveMorePages();
    }
}

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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.transform;

// A queue with constant time contains(E) and log time remove(E)
@ThreadSafe
final class FifoQueue<E>
        implements UpdateablePriorityQueue<E>
{
    private final Map<E, Entry<E>> map = new ConcurrentHashMap<>();
    private final Set<Entry<E>> queue = new ConcurrentSkipListSet<>(Comparator.comparingLong(Entry::getSequence));

    @GuardedBy("this")
    private long sequence;

    @Override
    public synchronized boolean addOrUpdate(E element, long priority)
    {
        if (contains(element)) {
            return false;
        }
        Entry<E> entry = new Entry<>(element, sequence);
        sequence++;
        map.put(element, entry);
        checkState(queue.add(entry), "Current queue already contains the element");
        return true;
    }

    @Override
    public boolean contains(E element)
    {
        return map.containsKey(element);
    }

    @Override
    public synchronized boolean remove(E element)
    {
        Entry entry = map.remove(element);
        if (entry == null) {
            return false;
        }
        checkState(queue.remove(entry), "Current queue does not contain the element");
        return true;
    }

    @Override
    public synchronized E poll()
    {
        Iterator<Entry<E>> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        E element = iterator.next().getValue();
        iterator.remove();
        checkState(map.remove(element) != null, "Failed to remove entry from the queue");
        return element;
    }

    @Override
    public E peek()
    {
        Iterator<Entry<E>> iterator = queue.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        return iterator.next().getValue();
    }

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    @Override
    public Iterator<E> iterator()
    {
        return transform(queue.iterator(), Entry::getValue);
    }

    private static final class Entry<E>
    {
        private final E value;
        private final long sequence;

        Entry(E value, long sequence)
        {
            this.value = value;
            this.sequence = sequence;
        }

        public E getValue()
        {
            return value;
        }

        long getSequence()
        {
            return sequence;
        }
    }
}

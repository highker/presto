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
package com.facebook.presto.array;

import io.airlift.slice.SizeOf;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.openjdk.jol.info.ClassLayout;

// This class tracks how many times an objects have been referenced
// in order not to over count memory in complex objects such as SliceBigArray or BlockBigArray.
// Ideally the key set should be the object references themselves.
// But it turns out such implementation can lead to GC performance issues.
// With several attempts including breaking down the object arrays into object big arrays,
// the problem can be solved but tracking overhead is still high.
// Benchmark results show using hash maps with primitive arrays perform way better than object arrays.
// Therefore, we use identity hash code + size of an object to identify an object.
// This may lead to hash collision but neglectable.
public final class ReferenceCountMap
        extends Long2IntOpenHashMap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ReferenceCountMap.class).instanceSize();

    /**
     * Increments the reference count of an object by 1 and returns the updated reference count
     */
    public int incrementAndGet(Object key, int size)
    {
        return addTo(getHashCode(key, size), 1) + 1;
    }

    /**
     * Decrements the reference count of an object by 1 and returns the updated reference count
     */
    public int decrementAndGet(Object key, int size)
    {
        long hashCode = getHashCode(key, size);
        int previousCount = addTo(hashCode, -1);
        if (previousCount == 1) {
            remove(hashCode);
        }
        return previousCount - 1;
    }

    /**
     * Returns the size of this map in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(key) + SizeOf.sizeOf(value) + SizeOf.sizeOf(used);
    }

    private static long getHashCode(Object key, int size)
    {
        // identityHashCode does not guarantee the uniqueness of an object.
        // Use size as an extra identity.
        return (((long) System.identityHashCode(key)) << Integer.SIZE) + size;
    }
}

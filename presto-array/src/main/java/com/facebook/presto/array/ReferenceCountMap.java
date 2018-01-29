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

import com.facebook.presto.spi.PrestoException;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class ReferenceCountMap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ReferenceCountMap.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;

    private int objectCount;
    private int hashCapacity;
    private int maxFill;
    private int mask;

    // the hash table from objects to reference counts
    private ObjectBigArray<Object> objects;
    private IntBigArray references;

    public ReferenceCountMap()
    {
        hashCapacity = arraySize(4, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        objects = new ObjectBigArray<>();
        objects.ensureCapacity(hashCapacity);
        references = new IntBigArray(-1);
        references.ensureCapacity(hashCapacity);
    }

    public long sizeOf()
    {
        return INSTANCE_SIZE + objects.sizeOf() + references.sizeOf();
    }

    public int incrementAndGet(Object object)
    {
        // look for an empty slot or a slot containing this key
        int hashPosition = getHashPosition(object, mask);
        while (true) {
            if (objects.get(hashPosition) == null) {
                break;
            }

            if (object == objects.get(hashPosition)) {
                int newCount = references.get(hashPosition) + 1;
                references.set(hashPosition, newCount);
                return newCount;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
        objects.set(hashPosition, object);
        objectCount++;

        // increase capacity, if necessary
        if (objectCount >= maxFill) {
            rehash();
        }
        return 1;
    }

    public int decrementAndGet(Object object)
    {
        // look for an empty slot or a slot containing this key
        int hashPosition = getHashPosition(object, mask);
        while (true) {
            if (objects.get(hashPosition) == null) {
                throw new IllegalArgumentException(format("didn't find object %s in the reference count map", object));
            }

            if (object == objects.get(hashPosition)) {
                int newCount = references.get(hashPosition) - 1;
                if (newCount == 0) {
                    objects.set(hashPosition, null);
                    objectCount--;
                }
                return newCount;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        int newMask = newCapacity - 1;
        ObjectBigArray<Object> newObjects = new ObjectBigArray<>();
        newObjects.ensureCapacity(newCapacity);
        IntBigArray newReferences = new IntBigArray();
        newReferences.ensureCapacity(newCapacity);

        for (int i = 0; i < hashCapacity; i++) {
            Object object = objects.get(i);
            if (object == null) {
                continue;
            }

            int referenceCount = references.get(i);
            // find an empty slot for the address
            long hashPosition = getHashPosition(object, newMask);
            while (newObjects.get(hashPosition) != null) {
                hashPosition = (hashPosition + 1) & newMask;
            }

            // record the mapping
            newObjects.set(hashPosition, object);
            newReferences.set(hashPosition, referenceCount);
        }

        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        objects = newObjects;
        references = newReferences;
    }

    private static int getHashPosition(Object object, int mask)
    {
        if (object == null) {
            throw new IllegalArgumentException("object is null");
        }

        int hashCode = System.identityHashCode(object);
        return murmurHash3(hashCode) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        if (hashSize <= 0) {
            throw new IllegalArgumentException("hashSize must be greater than 0");
        }
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        if (hashSize <= maxFill) {
            throw new IllegalArgumentException("hashSize must be larger than maxFill");
        }
        return maxFill;
    }
}

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
package com.facebook.presto.spi.block;

import org.openjdk.jol.info.ClassLayout;

import java.util.function.BiConsumer;

public class EmptyBlock
        implements Block
{
    public static final Block EMPTY_BLOCK = new EmptyBlock();

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(EmptyBlock.class).instanceSize();

    private EmptyBlock() {}

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        throw new IllegalArgumentException("position is not valid");
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        throw new IllegalArgumentException("position is not valid");
    }

    @Override
    public int getPositionCount()
    {
        return 0;
    }

    @Override
    public long getSizeInBytes()
    {
        return 0;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        if (position != 0 || length != 0) {
            throw new IllegalArgumentException("Empty block got non-zero position or length");
        }
        return 0;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(this, INSTANCE_SIZE);
    }

    @Override
    public BlockEncoding getEncoding()
    {
        return new EmptyBlockEncoding();
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        if (offset != 0 || length != 0) {
            throw new IllegalArgumentException("Empty block got non-zero offset or length");
        }
        return EMPTY_BLOCK;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        if (positionOffset != 0 || length != 0) {
            throw new IllegalArgumentException("Empty block got non-zero positionOffset or length");
        }
        return EMPTY_BLOCK;
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        if (position != 0 || length != 0) {
            throw new IllegalArgumentException("Empty block got non-zero position or length");
        }
        return EMPTY_BLOCK;
    }

    @Override
    public boolean isNull(int position)
    {
        throw new IllegalArgumentException("position is not valid");
    }
}

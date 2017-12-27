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

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.EmptyBlock.EMPTY_BLOCK;

public class EmptyBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<EmptyBlockEncoding> FACTORY = new EmptyBlockEncodingFactory();

    private static final String NAME = "EMPTY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        if (block.getPositionCount() != 0) {
            throw new IllegalArgumentException("Encoding empty block got non-zero position count");
        }
        sliceOutput.appendInt(0);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        if (sliceInput.readInt() != 0) {
            throw new IllegalArgumentException("Decoding empty block got non-zero position count");
        }
        return EMPTY_BLOCK;
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class EmptyBlockEncodingFactory
            implements BlockEncodingFactory<EmptyBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public EmptyBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            return new EmptyBlockEncoding();
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, EmptyBlockEncoding blockEncoding)
        {
        }
    }
}

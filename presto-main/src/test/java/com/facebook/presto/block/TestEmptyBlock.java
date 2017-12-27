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
package com.facebook.presto.block;

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.block.EmptyBlock.EMPTY_BLOCK;
import static org.testng.Assert.assertEquals;

public class TestEmptyBlock
        extends AbstractTestBlock
{
    @Test
    public void test()
    {
        assertBlock(EMPTY_BLOCK, new Slice[0]);
    }

    @Test
    public void testCopyAndGet()
            throws Exception
    {
        assertEquals(EMPTY_BLOCK.copyRegion(0, 0), EMPTY_BLOCK);
        assertEquals(EMPTY_BLOCK.getRegion(0, 0), EMPTY_BLOCK);
        assertEquals(EMPTY_BLOCK.copyPositions(new int[0], 0, 0), EMPTY_BLOCK);
        assertEquals(EMPTY_BLOCK.getPositions(new int[0]), EMPTY_BLOCK);
    }

    @Override
    protected boolean isShortAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isIntAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isLongAccessSupported()
    {
        return false;
    }

    @Override
    protected boolean isSliceAccessSupported()
    {
        return false;
    }
}

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
package com.facebook.presto.type;

import com.facebook.presto.spi.block.Block;
import io.airlift.slice.Slice;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class LazyRegexp
        implements AbstractRegexp
{
    private static final int MAX_PROFILING_COUNT = 4000;
    private static final int STOP_PROFILING_THRESHOLD = 10;
    private static final int RE2J_INDEX = 0;
    private static final int JONI_INDEX = 1;

    private final AbstractRegexp[] candidates = new AbstractRegexp[2];
    private final long[] elapsedNanos = new long[candidates.length];

    private AbstractRegexp delegatedRegexp;
    private int profilingCount;

    public LazyRegexp(Re2JRegexp re2JRegexp, JoniRegexp joniRegexp)
    {
        candidates[RE2J_INDEX] = requireNonNull(re2JRegexp, "re2JRegexp is null");
        candidates[JONI_INDEX] = requireNonNull(joniRegexp, "joniRegexp is null");
    }

    @Override
    public boolean matches(Slice source)
    {
        return runDelegate(regexp -> regexp.matches(source));
    }

    @Override
    public Slice replace(Slice source, Slice replacement)
    {
        return runDelegate(regexp -> regexp.replace(source, replacement));
    }

    @Override
    public Block extractAll(Slice source, long groupIndex)
    {
        return runDelegate(regexp -> regexp.extractAll(source, groupIndex));
    }

    @Override
    public Slice extract(Slice source, long groupIndex)
    {
        return runDelegate(regexp -> regexp.extract(source, groupIndex));
    }

    @Override
    public Block split(Slice source)
    {
        return runDelegate(regexp -> regexp.split(source));
    }

    private <R> R runDelegate(Function<AbstractRegexp, R> callable)
    {
        if (delegatedRegexp != null) {
            return callable.apply(delegatedRegexp);
        }

        profilingCount++;
        long start = System.nanoTime();
        R result = callable.apply(candidates[profilingCount % candidates.length]);
        tryDetermineDelegate(System.nanoTime() - start);
        return result;
    }

    private void tryDetermineDelegate(long duration)
    {
        elapsedNanos[profilingCount % candidates.length] += duration;

        if (profilingCount % 2 != 0) {
            return;
        }

        if (elapsedNanos[JONI_INDEX] > elapsedNanos[RE2J_INDEX] * STOP_PROFILING_THRESHOLD) {
            delegatedRegexp = candidates[RE2J_INDEX];
            return;
        }
        if (elapsedNanos[RE2J_INDEX] > elapsedNanos[JONI_INDEX] * STOP_PROFILING_THRESHOLD) {
            delegatedRegexp = candidates[JONI_INDEX];
            return;
        }

        if (profilingCount < MAX_PROFILING_COUNT) {
            return;
        }

        if (elapsedNanos[JONI_INDEX] > elapsedNanos[RE2J_INDEX]) {
            delegatedRegexp = candidates[RE2J_INDEX];
            return;
        }
        delegatedRegexp = candidates[JONI_INDEX];
    }
}

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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.type.AbstractRegexp;
import com.facebook.presto.type.JoniRegexp;
import com.facebook.presto.type.LazyRegexp;
import com.facebook.presto.type.Re2JRegexp;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
@OutputTimeUnit(NANOSECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 30)
public class RegexpFunctionsBenchmark
{
    @Benchmark
    public boolean benchmarkLikeJoni(DotStarAroundData data)
    {
        return RegexpFunctions.regexpLike(data.getSource(), data.getJoniPattern());
    }

    @Benchmark
    public boolean benchmarkLikeRe2J(DotStarAroundData data)
    {
        return RegexpFunctions.regexpLike(data.getSource(), data.getRe2JPattern());
    }

    @Benchmark
    public boolean benchmarkLikeLazy(DotStarAroundData data)
    {
        return RegexpFunctions.regexpLike(data.getSource(), data.getLazyPattern());
    }

    @State(Thread)
    public static class DotStarAroundData
    {
        @Param({
                // common ones:
                ".*x.*",
                ".*(x|y).*",
                "longdotstar",
                "phone",
                "literal",

                // re2j biased (100X - 100000X faster than joni):
                ".*\\+*123\\+* literal.*",
                "([0-9]+)@company\\.com",
                "^[1-9][0-9.]+$",
                ".*r(=|%3D)ab1.*",
                "([^(]*)",
                "([a-z]+)literal([\\d.]+)([a-z.0-9]+)",
                "(?i)\\w+\\s*lit\\s*",
                ".*literal1.*|.*someotherlit.*|.*differnt.*",

                // joni biased (10X - 100X faster than re2j):
                "^.+__long_long_long_literal_(\\\\d+)$",
                "(^[0-9]*$)|^[a-z]$|^([a-f0-9]{32})$",
                "^(\\[\")?[^\\[\"\\]]*,[^\\[\"\\]]*(\"\\])?$",
                "(^|[^0-9a-z])literal(^|[^0-9a-z])",
                "123456789012345:0:\"([\\\\w\\\\s\\\\.,]*)\"",
                ".+literal1.+literal2.+",

                // similar performance:
                "(lit_)([a-zA-Z_]+)",
                ".*literal(.*)",
                "123_\\D+",
                "literal ?ab ?literal",
                "ab|cdefg|hig|klm|nopq|rstuvw|xyz"
        })
        private String patternString;

        @Param({"1024", "32768"})
        private int sourceLength;

        private AbstractRegexp joniPattern;
        private AbstractRegexp re2JPattern;
        private AbstractRegexp lazyPattern;
        private Slice source;

        @Setup
        public void setup()
        {
            SliceOutput sliceOutput = new DynamicSliceOutput(sourceLength);
            Slice pattern;
            switch (patternString) {
                case ".*x.*":
                    pattern = Slices.utf8Slice(".*x.*");
                    IntStream.generate(() -> 97).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                case ".*(x|y).*":
                    pattern = Slices.utf8Slice(".*(x|y).*");
                    IntStream.generate(() -> 97).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                case "longdotstar":
                    pattern = Slices.utf8Slice(".*coolfunctionname.*");
                    ThreadLocalRandom.current().ints(97, 123).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                case "phone":
                    pattern = Slices.utf8Slice("\\d{3}/\\d{3}/\\d{4}");
                    // 47: '/', 48-57: '0'-'9'
                    ThreadLocalRandom.current().ints(47, 58).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                case "literal":
                    pattern = Slices.utf8Slice("literal");
                    // 97-122: 'a'-'z'
                    ThreadLocalRandom.current().ints(97, 123).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
                default:
                    pattern = Slices.utf8Slice(patternString);
                    ThreadLocalRandom.current().ints(31, 127).limit(sourceLength).forEach(sliceOutput::appendByte);
                    break;
            }

            joniPattern = joniRegexp(pattern);
            re2JPattern = re2JRegexp(pattern);
            lazyPattern = lazyRegexp(pattern);
            source = sliceOutput.slice();
            checkState(source.length() == sourceLength, "source.length=%s, sourceLength=%s", source.length(), sourceLength);
        }

        public Slice getSource()
        {
            return source;
        }

        public AbstractRegexp getJoniPattern()
        {
            return joniPattern;
        }

        public AbstractRegexp getRe2JPattern()
        {
            return re2JPattern;
        }

        public AbstractRegexp getLazyPattern()
        {
            return lazyPattern;
        }
    }

    private static AbstractRegexp joniRegexp(Slice pattern)
    {
        return new JoniRegexp(pattern);
    }

    private static AbstractRegexp re2JRegexp(Slice pattern)
    {
        return new Re2JRegexp(Integer.MAX_VALUE, 5, pattern);
    }

    private static AbstractRegexp lazyRegexp(Slice pattern)
    {
        return new LazyRegexp(new Re2JRegexp(Integer.MAX_VALUE, 5, pattern), new JoniRegexp(pattern));
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + RegexpFunctionsBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}

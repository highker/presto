package com.facebook.presto.spi.block;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkGetRegionSizeInBytes
{
    private static final int NUMBER_OF_RANGES = 1000;

    @State(Scope.Thread)
    public static class Data
    {
        @Param({"1000000", "10000", "100"})
        private String numberOfEntries = "100";

        private Block block;
        private Range[] ranges;

        @Setup
        public void setup()
        {
            int entries = Integer.parseInt(numberOfEntries);
            ranges = new Range[NUMBER_OF_RANGES];
            for (int i = 0; i < NUMBER_OF_RANGES; i++) {
                int offset = ThreadLocalRandom.current().nextInt(0, entries - 1);
                int length = ThreadLocalRandom.current().nextInt(0, entries - offset - 1);
                ranges[i] = new Range(offset, length);
            }

            long[] values = new long[entries];
            for (int i = 0; i < entries; i++) {
                values[i] = (long) (ThreadLocalRandom.current().nextGaussian());
            }
            block = new DictionaryBlock(new LongArrayBlock(entries, new boolean[entries], values), IntStream.range(0, entries).toArray());

        }
    }

    @Benchmark
    @OperationsPerInvocation(NUMBER_OF_RANGES)
    public long BenchmarkDictionaryGetRegionSizeInBytes(Data data)
    {
        long result = 0;
        for (Range range : data.ranges) {
            result += data.block.getRegionSizeInBytes(range.getOffset(), range.getLength());
        }
        return result;
    }

    @Benchmark
    @OperationsPerInvocation(NUMBER_OF_RANGES)
    public long BenchmarkDictionaryGetRegionSizeInBytesLengthOne(Data data)
    {
        long result = 0;
        for (Range range : data.ranges) {
            result += data.block.getRegionSizeInBytes(range.getOffset(), 1);
        }
        return result;
    }

    private static class Range
    {
        private final int offset;
        private final int length;

        public Range(int offset, int length)
        {
            this.offset = offset;
            this.length = length;
        }

        public int getOffset()
        {
            return offset;
        }

        public int getLength()
        {
            return length;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .include(".*" + BenchmarkGetRegionSizeInBytes.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}

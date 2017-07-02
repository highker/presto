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
package com.facebook.presto.orc.metadata.statistics;

public class ColumnStatistics
{
    private final Long numberOfValues;
    private final long minAverageValueSizeInBytes;
    private final BooleanStatistics booleanStatistics;
    private final IntegerStatistics integerStatistics;
    private final DoubleStatistics doubleStatistics;
    private final StringStatistics stringStatistics;
    private final DateStatistics dateStatistics;
    private final DecimalStatistics decimalStatistics;
    private final BinaryStatistics binaryStatistics;
    private final HiveBloomFilter bloomFilter;

    public ColumnStatistics(
            Long numberOfValues,
            long minAverageValueSizeInBytes,
            BooleanStatistics booleanStatistics,
            IntegerStatistics integerStatistics,
            DoubleStatistics doubleStatistics,
            StringStatistics stringStatistics,
            DateStatistics dateStatistics,
            DecimalStatistics decimalStatistics,
            BinaryStatistics binaryStatistics,
            HiveBloomFilter bloomFilter)
    {
        this.numberOfValues = numberOfValues;
        this.minAverageValueSizeInBytes = minAverageValueSizeInBytes;
        this.booleanStatistics = booleanStatistics;
        this.integerStatistics = integerStatistics;
        this.doubleStatistics = doubleStatistics;
        this.stringStatistics = stringStatistics;
        this.dateStatistics = dateStatistics;
        this.decimalStatistics = decimalStatistics;
        this.binaryStatistics = binaryStatistics;
        this.bloomFilter = bloomFilter;
    }

    public boolean hasNumberOfValues()
    {
        return numberOfValues != null;
    }

    public long getNumberOfValues()
    {
        return numberOfValues == null ? 0 : numberOfValues;
    }

    public boolean hasMinAverageValueSizeInBytes()
    {
        return hasNumberOfValues() && numberOfValues > 0;
    }

    /**
     * The minimum average value sizes.
     * The actual average value size is no less than the return value.
     * It provides a lower bound of the size of data to be loaded
     */
    public long getMinAverageValueSizeInBytes()
    {
        // it is ok to return 0 if the size does not exist given it is a lower bound
        return minAverageValueSizeInBytes;
    }

    public BooleanStatistics getBooleanStatistics()
    {
        return booleanStatistics;
    }

    public DateStatistics getDateStatistics()
    {
        return dateStatistics;
    }

    public DoubleStatistics getDoubleStatistics()
    {
        return doubleStatistics;
    }

    public IntegerStatistics getIntegerStatistics()
    {
        return integerStatistics;
    }

    public StringStatistics getStringStatistics()
    {
        return stringStatistics;
    }

    public DecimalStatistics getDecimalStatistics()
    {
        return decimalStatistics;
    }

    public BinaryStatistics getBinaryStatistics()
    {
        return binaryStatistics;
    }

    public HiveBloomFilter getBloomFilter()
    {
        return bloomFilter;
    }

    public ColumnStatistics withBloomFilter(HiveBloomFilter bloomFilter)
    {
        return new ColumnStatistics(
                numberOfValues,
                minAverageValueSizeInBytes,
                booleanStatistics,
                integerStatistics,
                doubleStatistics,
                stringStatistics,
                dateStatistics,
                decimalStatistics,
                binaryStatistics,
                bloomFilter);
    }
}

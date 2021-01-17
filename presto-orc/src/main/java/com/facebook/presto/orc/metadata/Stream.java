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
package com.facebook.presto.orc.metadata;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Stream
{
    public enum StreamKind
    {
        PRESENT((byte) 0),
        DATA((byte) 1),
        LENGTH((byte) 2),
        DICTIONARY_DATA((byte) 3),
        DICTIONARY_COUNT((byte) 4),
        SECONDARY((byte) 5),
        ROW_INDEX((byte) 6),
        BLOOM_FILTER((byte) 7),
        BLOOM_FILTER_UTF8((byte) 8),
        IN_DICTIONARY((byte) 9),
        ROW_GROUP_DICTIONARY((byte) 10),
        ROW_GROUP_DICTIONARY_LENGTH((byte) 11),
        IN_MAP((byte) 12);

        private final byte value;

        StreamKind(byte value)
        {
            this.value = value;
        }

        public byte getValue()
        {
            return value;
        }

        public static Stream.StreamKind createStreamKind(byte value)
        {
            switch (value) {
                case 0:
                    return PRESENT;
                case 1:
                    return DATA;
                case 2:
                    return LENGTH;
                case 3:
                    return DICTIONARY_DATA;
                case 4:
                    return DICTIONARY_COUNT;
                case 5:
                    return SECONDARY;
                case 6:
                    return ROW_INDEX;
                case 7:
                    return BLOOM_FILTER;
                case 8:
                    return BLOOM_FILTER_UTF8;
                case 9:
                    return IN_DICTIONARY;
                case 10:
                    return ROW_GROUP_DICTIONARY;
                case 11:
                    return ROW_GROUP_DICTIONARY_LENGTH;
                case 12:
                    return IN_MAP;
            }
            throw new RuntimeException();
        }
    }

    private final int column;
    private final StreamKind streamKind;
    private final int length;
    private final boolean useVInts;
    private final int sequence;

    private final Optional<Long> offset;

    public Stream(int column, StreamKind streamKind, int length, boolean useVInts)
    {
        this(column, streamKind, length, useVInts, ColumnEncoding.DEFAULT_SEQUENCE_ID, Optional.empty());
    }

    public Stream(int column, StreamKind streamKind, int length, boolean useVInts, int sequence, Optional<Long> offset)
    {
        this.column = column;
        this.streamKind = requireNonNull(streamKind, "streamKind is null");
        this.length = length;
        this.useVInts = useVInts;
        this.sequence = sequence;
        this.offset = requireNonNull(offset, "offset is null");
    }

    public int getColumn()
    {
        return column;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    public int getLength()
    {
        return length;
    }

    public boolean isUseVInts()
    {
        return useVInts;
    }

    public int getSequence()
    {
        return sequence;
    }

    public Optional<Long> getOffset()
    {
        return offset;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("streamKind", streamKind)
                .add("length", length)
                .add("useVInts", useVInts)
                .add("sequence", sequence)
                .add("offset", offset)
                .toString();
    }

    public Stream withOffset(long offset)
    {
        return new Stream(
                this.column,
                this.streamKind,
                this.length,
                this.useVInts,
                this.sequence,
                Optional.of(offset));
    }
}

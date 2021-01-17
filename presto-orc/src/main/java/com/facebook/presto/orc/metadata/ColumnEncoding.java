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
import java.util.SortedMap;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ColumnEncoding
{
    public enum ColumnEncodingKind
    {
        DIRECT((byte) 0),
        DICTIONARY((byte) 1),
        DIRECT_V2((byte) 2),
        DICTIONARY_V2((byte) 3),
        DWRF_DIRECT((byte) 4),
        DWRF_MAP_FLAT((byte) 5);

        private final byte value;

        ColumnEncodingKind(byte value)
        {
            this.value = value;
        }

        public byte getValue()
        {
            return value;
        }

        public static ColumnEncodingKind createColumnEncodingKind(byte value)
        {
            switch (value) {
                case 0:
                    return DIRECT;
                case 1:
                    return DICTIONARY;
                case 2:
                    return DIRECT_V2;
                case 3:
                    return DICTIONARY_V2;
                case 4:
                    return DWRF_DIRECT;
                case 5:
                    return DWRF_MAP_FLAT;
            }
            throw new RuntimeException();
        }
    }

    public static final int DEFAULT_SEQUENCE_ID = 0;

    private final ColumnEncodingKind columnEncodingKind;
    private final int dictionarySize;

    // DWRF supports the concept of sequences.
    // A column can be modeled as multiple sequences that are independently encoded.
    // For example, for a flat map column, each key will have a
    // separate value stream with its own column encoding.
    // These additional sequence IDs start from 1 and may not be consecutive, for example, the file may be updated to
    // remove keys from the flat map without changing the sequence IDs associated with each key.
    // Sorted so that when we iterate over the map the DwrfSequenceEncodings are returned in ascending Sequence ID order
    private final Optional<SortedMap<Integer, DwrfSequenceEncoding>> additionalSequenceEncodings;

    public ColumnEncoding(ColumnEncodingKind columnEncodingKind, int dictionarySize)
    {
        this(columnEncodingKind, dictionarySize, Optional.empty());
    }

    public ColumnEncoding(ColumnEncodingKind columnEncodingKind, int dictionarySize, Optional<SortedMap<Integer, DwrfSequenceEncoding>> additionalSequenceEncodings)
    {
        this.columnEncodingKind = requireNonNull(columnEncodingKind, "columnEncodingKind is null");
        this.dictionarySize = dictionarySize;
        this.additionalSequenceEncodings = additionalSequenceEncodings;
    }

    public ColumnEncodingKind getColumnEncodingKind()
    {
        return columnEncodingKind;
    }

    public int getDictionarySize()
    {
        return dictionarySize;
    }

    public Optional<SortedMap<Integer, DwrfSequenceEncoding>> getAdditionalSequenceEncodings()
    {
        return additionalSequenceEncodings;
    }

    public ColumnEncoding getColumnEncoding(int sequence)
    {
        if (sequence == 0) {
            return this;
        }

        checkState(
                additionalSequenceEncodings.isPresent(),
                "Got non-zero sequence: %d, but there are no additional sequence encodings: %s", sequence, this);

        DwrfSequenceEncoding sequenceEncoding = additionalSequenceEncodings.get().get(sequence);

        checkState(
                sequenceEncoding != null,
                "Non-zero sequence %d is not present in the ColumnEncoding's additional sequences: %s",
                sequence,
                additionalSequenceEncodings.get().keySet());

        return sequenceEncoding.getValueEncoding();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnEncodingKind", columnEncodingKind)
                .add("dictionarySize", dictionarySize)
                .add("additionalSequenceEncodings", additionalSequenceEncodings)
                .toString();
    }
}

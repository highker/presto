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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

import static java.util.Objects.requireNonNull;

public class StripeFooter
{
    private final int[] columns;
    private final byte[] streamKinds;
    private final int[] lengths;
    private final boolean[] useVInts;
    private final int[] sequences;
    private final long[] offsets;
    private final boolean[] hasOffsets;

    private final Map<Integer, Integer> columnEncodingsForDictionary;
    private final Map<Integer, Byte> columnEncodingsForKind;
    private final Map<Integer, SortedMap<Integer, DwrfSequenceEncoding>> columnEncodingsForAdditional;

    // encrypted StripeEncryptionGroups
    private final List<Slice> stripeEncryptionGroups;

    public StripeFooter(List<Stream> streams, Map<Integer, ColumnEncoding> columnEncodings, List<Slice> stripeEncryptionGroups)
    {
        requireNonNull(streams, "streams is null");
        this.columns = new int[streams.size()];
        this.streamKinds = new byte[streams.size()];
        this.lengths = new int[streams.size()];
        this.useVInts = new boolean[streams.size()];
        this.sequences = new int[streams.size()];
        this.offsets = new long[streams.size()];
        this.hasOffsets = new boolean[streams.size()];
        for (int i = 0; i < streams.size(); i++) {
            columns[i] = streams.get(i).getColumn();
            streamKinds[i] = streams.get(i).getStreamKind().getValue();
            lengths[i] = streams.get(i).getLength();
            useVInts[i] = streams.get(i).isUseVInts();
            sequences[i] = streams.get(i).getSequence();
            offsets[i] = streams.get(i).getOffset().orElse(0L);
            hasOffsets[i] = streams.get(i).getOffset().isPresent();
        }

        requireNonNull(columnEncodings, "columnEncodings is null");
        this.columnEncodingsForDictionary = new Int2IntOpenHashMap(Maps.transformValues(columnEncodings, ColumnEncoding::getDictionarySize));
        this.columnEncodingsForKind = new Int2ByteOpenHashMap(Maps.transformValues(columnEncodings, v -> v.getColumnEncodingKind().getValue()));
        this.columnEncodingsForAdditional = new Int2ObjectOpenHashMap<>(Maps.transformValues(columnEncodings, v -> v.getAdditionalSequenceEncodings().orElse(null)));
        this.stripeEncryptionGroups = ImmutableList.copyOf(requireNonNull(stripeEncryptionGroups, "stripeEncryptionGroups is null"));
    }

    public Map<Integer, ColumnEncoding> getColumnEncodings(Set<Integer> includedOrcColumns)
    {
        Map<Integer, ColumnEncoding> result = new HashMap<>();
        for (int column : includedOrcColumns) {
            result.put(column, new ColumnEncoding(
                    ColumnEncoding.ColumnEncodingKind.createColumnEncodingKind(columnEncodingsForKind.get(column)),
                    columnEncodingsForDictionary.get(column),
                    Optional.ofNullable(columnEncodingsForAdditional.get(column))));
        }
        return result;
    }

    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        Map<Integer, ColumnEncoding> result = new HashMap<>();
        for (Map.Entry<Integer, Byte> entry : columnEncodingsForKind.entrySet()) {
            result.put(entry.getKey(), new ColumnEncoding(
                    ColumnEncoding.ColumnEncodingKind.createColumnEncodingKind(entry.getValue()),
                    columnEncodingsForDictionary.get(entry.getKey()),
                    Optional.ofNullable(columnEncodingsForAdditional.get(entry.getKey()))));
        }
        return result;
    }

    public List<Stream> getStreams(Set<Integer> includedOrcColumns)
    {
        List<Stream> result = new ArrayList<>(includedOrcColumns.size() * 2);
        for (int i = 0; i < columns.length; i++) {
            if (!includedOrcColumns.contains(columns[i])) {
                continue;
            }
            result.add(new Stream(
                    columns[i],
                    Stream.StreamKind.createStreamKind(streamKinds[i]),
                    lengths[i],
                    useVInts[i],
                    sequences[i],
                    hasOffsets[i] ? Optional.of(offsets[i]) : Optional.empty()));
        }
        return result;
    }

    public List<Stream> getStreams()
    {
        List<Stream> result = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            result.add(new Stream(
                    columns[i],
                    Stream.StreamKind.createStreamKind(streamKinds[i]),
                    lengths[i],
                    useVInts[i],
                    sequences[i],
                    hasOffsets[i] ? Optional.of(offsets[i]) : Optional.empty()));
        }
        return result;
    }

    public List<Slice> getStripeEncryptionGroups()
    {
        return stripeEncryptionGroups;
    }

    public int[] getColumns()
    {
        return columns;
    }

    public byte[] getStreamKinds()
    {
        return streamKinds;
    }

    public int[] getLengths()
    {
        return lengths;
    }

    public boolean[] getUseVInts()
    {
        return useVInts;
    }

    public int[] getSequences()
    {
        return sequences;
    }

    public long[] getOffsets()
    {
        return offsets;
    }

    public boolean[] getHasOffsets()
    {
        return hasOffsets;
    }
}

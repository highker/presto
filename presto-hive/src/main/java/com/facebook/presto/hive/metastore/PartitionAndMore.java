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
package com.facebook.presto.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Immutable
public class PartitionAndMore
{
    private final Partition partition;
    private final Path currentLocation;
    private final Optional<List<String>> fileNames;

    public PartitionAndMore(Partition partition, Path currentLocation, Optional<List<String>> fileNames)
    {
        this.partition = requireNonNull(partition, "partition is null");
        this.currentLocation = requireNonNull(currentLocation, "currentLocation is null");
        this.fileNames = requireNonNull(fileNames, "fileNames is null");
    }

    @JsonCreator
    public PartitionAndMore(
            @JsonProperty("partition") Partition partition,
            @JsonProperty("currentLocationUri") URI currentLocationUri,
            @JsonProperty("optionalFileNames") Optional<List<String>> optionalFileNames)
    {
        this(partition, new Path(requireNonNull(currentLocationUri, "currentLocationUri is null")), optionalFileNames);
    }

    @JsonProperty
    public Partition getPartition()
    {
        return partition;
    }

    Partition getAugmentedPartitionForInTransactionRead()
    {
        // This method augments the location field of the partition to the staging location.
        // This way, if the partition is accessed in an ongoing transaction, staged data
        // can be found and accessed.
        Partition partition = this.partition;
        String currentLocation = this.currentLocation.toString();
        if (!currentLocation.equals(partition.getStorage().getLocation())) {
            partition = Partition.builder(partition)
                    .withStorage(storage -> storage.setLocation(currentLocation))
                    .build();
        }
        return partition;
    }

    public Path getCurrentLocation()
    {
        return currentLocation;
    }

    @JsonProperty
    public URI getCurrentLocationUri()
    {
        return currentLocation.toUri();
    }

    @JsonProperty
    public Optional<List<String>> getOptionalFileNames()
    {
        return fileNames;
    }

    public List<String> getFileNames()
    {
        checkState(fileNames.isPresent());
        return fileNames.get();
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        PartitionAndMore otherPartitionAndMore = (PartitionAndMore) other;
        return Objects.equals(partition, otherPartitionAndMore.partition) &&
                Objects.equals(currentLocation, otherPartitionAndMore.currentLocation) &&
                Objects.equals(fileNames, otherPartitionAndMore.fileNames);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partition, currentLocation, fileNames);
    }
}

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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableAndMore
{
    private final Table table;
    private final Optional<PrincipalPrivileges> principalPrivileges;
    private final Optional<Path> currentLocation; // unpartitioned table only
    private final Optional<List<String>> fileNames;
    private final boolean ignoreExisting;

    public TableAndMore(
            Table table,
            Optional<PrincipalPrivileges> principalPrivileges,
            Optional<Path> currentLocation,
            Optional<List<String>> fileNames,
            boolean ignoreExisting)
    {
        this.table = requireNonNull(table, "table is null");
        this.principalPrivileges = requireNonNull(principalPrivileges, "principalPrivileges is null");
        this.currentLocation = requireNonNull(currentLocation, "currentLocation is null");
        this.fileNames = requireNonNull(fileNames, "fileNames is null");
        this.ignoreExisting = ignoreExisting;

        checkArgument(!table.getStorage().getLocation().isEmpty() || !currentLocation.isPresent(), "currentLocation can not be supplied for table without location");
        checkArgument(!fileNames.isPresent() || currentLocation.isPresent(), "fileNames can be supplied only when currentLocation is supplied");
    }

    @JsonCreator
    public TableAndMore(
            @JsonProperty("table") Table table,
            @JsonProperty("optionalPrincipalPrivileges") Optional<PrincipalPrivileges> optionalPrincipalPrivileges,
            @JsonProperty("currentLocationUri") URI currentLocationUri,
            @JsonProperty("fileNames") Optional<List<String>> fileNames,
            @JsonProperty("ignoreExisting") boolean ignoreExisting)
    {
        this(table, optionalPrincipalPrivileges, Optional.ofNullable(currentLocationUri).map(Path::new), fileNames, ignoreExisting);
    }

    @JsonProperty
    public boolean isIgnoreExisting()
    {
        return ignoreExisting;
    }

    @JsonProperty
    public Table getTable()
    {
        return table;
    }

    public PrincipalPrivileges getPrincipalPrivileges()
    {
        checkState(principalPrivileges.isPresent());
        return principalPrivileges.get();
    }

    @JsonProperty
    public Optional<PrincipalPrivileges> getOptionalPrincipalPrivileges()
    {
        return principalPrivileges;
    }

    public Optional<Path> getCurrentLocation()
    {
        return currentLocation;
    }

    @JsonProperty
    public URI getCurrentLocationUri()
    {
        return currentLocation.map(Path::toUri).orElse(null);
    }

    @JsonProperty
    public Optional<List<String>> getFileNames()
    {
        return fileNames;
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
        TableAndMore otherTableAndMore = (TableAndMore) other;
        return Objects.equals(table, otherTableAndMore.table) &&
                Objects.equals(principalPrivileges, otherTableAndMore.principalPrivileges) &&
                Objects.equals(currentLocation, otherTableAndMore.currentLocation) &&
                Objects.equals(fileNames, otherTableAndMore.fileNames) &&
                ignoreExisting == otherTableAndMore.ignoreExisting;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, principalPrivileges, currentLocation, fileNames, ignoreExisting);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("principalPrivileges", principalPrivileges)
                .add("currentLocation", currentLocation)
                .toString();
    }
}

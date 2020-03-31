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
package org.apache.hadoop.fs;

import com.facebook.presto.hive.util.DirectoryLister;
import com.facebook.presto.hive.util.HadoopDirectoryLister;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PrestoExtendedFileSystemCache
        extends PrestoFileSystemCache
{
    private Optional<DirectoryLister> directoryLister = Optional.empty();

    public synchronized void setDirectoryLister(DirectoryLister directoryLister)
    {
        this.directoryLister = Optional.of(requireNonNull(directoryLister, "directoryLister is null"));
    }

    @Override
    protected FileSystem createPrestoFileSystemWrapper(FileSystem original)
    {
        return new HadoopExtendedFileSystemWrapper(original, directoryLister.orElse(new HadoopDirectoryLister()));
    }
}

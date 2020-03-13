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
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.NestedDirectoryPolicy;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Table;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.util.Iterator;

public class HadoopDirectoryLister
        implements DirectoryLister
{
    @Override
    public Iterator<HiveFileInfo> list(
            ExtendedFileSystem fileSystem,
            Table table,
            Path path,
            NamenodeStats namenodeStats,
            NestedDirectoryPolicy nestedDirectoryPolicy,
            PathFilter pathFilter)
    {
        return new HiveFileIterator(path, p -> new HadoopFileInfoIterator(fileSystem.listLocatedStatus(p)), namenodeStats, nestedDirectoryPolicy, pathFilter);
    }
}

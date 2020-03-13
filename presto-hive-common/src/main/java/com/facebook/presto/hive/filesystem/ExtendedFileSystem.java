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
package com.facebook.presto.hive.filesystem;

import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.NamenodeStats;
import com.facebook.presto.hive.NestedDirectoryPolicy;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.DirectoryLister;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.Iterator;

public abstract class ExtendedFileSystem
        extends FileSystem
{
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        return open(path);
    }

    public Iterator<HiveFileInfo> list(
            DirectoryLister directoryLister,
            Table table,
            Path path,
            NamenodeStats namenodeStats,
            NestedDirectoryPolicy nestedDirectoryPolicy,
            PathFilter pathFilter)
    {
        return directoryLister.list(this, table, path, namenodeStats, nestedDirectoryPolicy, pathFilter);
    }

    public RemoteIterator<LocatedFileStatus> listDirectory(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public RemoteIterator<HiveFileInfo> listFiles(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public RemoteIterator<LocatedFileStatus> listByPrefix(Path path)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}

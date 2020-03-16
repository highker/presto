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
package com.facebook.presto.cache;

import com.facebook.presto.hive.ExtendedFileSystem;
import com.facebook.presto.hive.HadoopFileSystem;
import com.facebook.presto.hive.HiveFileContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import static java.util.Objects.requireNonNull;

public final class CachingFileSystem
        extends HadoopFileSystem
{
    private final CacheManager cacheManager;
    private final ExtendedFileSystem dataTier;
    private final boolean cacheValidationEnabled;

    public CachingFileSystem(
            URI uri,
            Configuration configuration,
            CacheManager cacheManager,
            ExtendedFileSystem dataTier,
            boolean cacheValidationEnabled)
    {
        super(uri, dataTier);
        requireNonNull(configuration, "configuration is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.dataTier = requireNonNull(dataTier, "dataTier is null");
        this.cacheValidationEnabled = cacheValidationEnabled;

        setConf(configuration);

        //noinspection AssignmentToSuperclassField
        statistics = getStatistics(uri.getScheme(), getClass());
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        return new CachingInputStream(dataTier.open(path, bufferSize), cacheManager, path, cacheValidationEnabled);
    }

    // copied from CachingFileOpener
    @Override
    public FSDataInputStream openFileByDescriptor(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        FSDataInputStream inputStream = dataTier.openFileByDescriptor(path, hiveFileContext);
        if (hiveFileContext.isCacheable()) {
            return new CachingInputStream(inputStream, cacheManager, path, isCacheValidationEnabled());
        }
        return inputStream;
    }

    public FileSystem getDataTier()
    {
        return dataTier;
    }

    public boolean isCacheValidationEnabled()
    {
        return cacheValidationEnabled;
    }
}

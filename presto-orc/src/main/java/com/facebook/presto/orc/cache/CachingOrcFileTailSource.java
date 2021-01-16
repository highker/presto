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
package com.facebook.presto.orc.cache;

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcDataSourceId;
import com.facebook.presto.orc.OrcWriteValidation;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.units.Duration;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class CachingOrcFileTailSource
        implements OrcFileTailSource
{
    private static final Duration EXPIRE_AFTER_ACCESS = new Duration(100, MINUTES);

    private final Cache<OrcDataSourceId, OrcFileTail> cache;
    private final Cache<OrcDataSourceId, Footer> footerCache;
    private final OrcFileTailSource delegate;

    public CachingOrcFileTailSource(OrcFileTailSource delegate, Cache<OrcDataSourceId, OrcFileTail> cache)
    {
        this.cache = requireNonNull(cache, "cache is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.footerCache = CacheBuilder.newBuilder()
                .expireAfterAccess(EXPIRE_AFTER_ACCESS.toMillis(), MILLISECONDS)
                .maximumSize(1_000_000)
                .build();
    }

    public Cache<OrcDataSourceId, Footer> getFooterCache()
    {
        return footerCache;
    }

    @Override
    public OrcFileTail getOrcFileTail(OrcDataSource orcDataSource, MetadataReader metadataReader, Optional<OrcWriteValidation> writeValidation, boolean cacheable)
            throws IOException
    {
        try {
            if (cacheable) {
                return cache.get(orcDataSource.getId(), () -> delegate.getOrcFileTail(orcDataSource, metadataReader, writeValidation, cacheable));
            }
            return delegate.getOrcFileTail(orcDataSource, metadataReader, writeValidation, cacheable);
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw new IOException("Unexpected error in orc file tail reading after cache miss", e.getCause());
        }
    }
}

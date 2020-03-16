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
package com.facebook.presto.hive;

import com.facebook.presto.hadoop.FileSystemFactory;
import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.function.BiFunction;

import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static com.facebook.presto.hive.util.ConfigurationUtils.getInitialConfiguration;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class HiveHdfsConfiguration
        implements HdfsConfiguration
{
    private static final Configuration INITIAL_CONFIGURATION = getInitialConfiguration();

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration configuration = new Configuration(false);
            copy(INITIAL_CONFIGURATION, configuration);
            initializer.updateConfiguration(configuration);
            return configuration;
        }
    };

    private final HdfsConfigurationInitializer initializer;
    private final Set<DynamicConfigurationProvider> dynamicProviders;

    @Inject
    public HiveHdfsConfiguration(HdfsConfigurationInitializer initializer, Set<DynamicConfigurationProvider> dynamicProviders)
    {
        this.initializer = requireNonNull(initializer, "initializer is null");
        this.dynamicProviders = ImmutableSet.copyOf(requireNonNull(dynamicProviders, "dynamicProviders is null"));
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        @SuppressWarnings("resource")
        Configuration config = new HiveHdfsConfiguration.HadoopJobConf((factoryConfig, factoryUri) -> {
            try {
                return new HadoopFileSystem(factoryUri, (new Path(uri)).getFileSystem(hadoopConfiguration.get()));
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "cannot create caching file system", e);
            }
        });

        copy(hadoopConfiguration.get(), config);
        for (DynamicConfigurationProvider provider : dynamicProviders) {
            provider.updateConfiguration(config, context, uri);
        }

        return config;
    }

    private static class HadoopJobConf
            extends JobConf
            implements FileSystemFactory
    {
        private final BiFunction<Configuration, URI, FileSystem> factory;

        private HadoopJobConf(BiFunction<Configuration, URI, FileSystem> factory)
        {
            super(false);
            this.factory = requireNonNull(factory, "factory is null");
        }

        @Override
        public FileSystem createFileSystem(URI uri)
        {
            return factory.apply(this, uri);
        }
    }
}

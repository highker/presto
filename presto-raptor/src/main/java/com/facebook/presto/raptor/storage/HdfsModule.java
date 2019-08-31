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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.raptor.filesystem.RaptorHdfsConfig;
import com.facebook.presto.raptor.filesystem.RaptorHdfsConfiguration;
import com.facebook.presto.raptor.filesystem.RaptorRemoteFileSystemConfiguration;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class HdfsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(RaptorHdfsConfig.class);
        binder.bind(RaptorRemoteFileSystemConfiguration.class).to(RaptorHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(StorageService.class).to(HdfsStorageService.class).in(Scopes.SINGLETON);
        binder.bind(OrcDataEnvironment.class).to(HdfsOrcDataEnvironment.class).in(Scopes.SINGLETON);
    }
}

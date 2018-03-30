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
package com.facebook.presto.server;

import com.facebook.presto.spi.NodeType;
import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import static com.facebook.presto.spi.NodeType.COORDINATOR;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ServerConfig
{
    private NodeType nodeType = COORDINATOR;
    private String prestoVersion;
    private String dataSources;
    private boolean includeExceptionInResponse = true;
    private Duration gracePeriod = new Duration(2, MINUTES);

    public NodeType getNodeType()
    {
        return nodeType;
    }

    @Config("node-type")
    public ServerConfig setNodeType(NodeType nodeType)
    {
        this.nodeType = nodeType;
        return this;
    }

    public String getPrestoVersion()
    {
        return prestoVersion;
    }

    @Config("presto.version")
    public ServerConfig setPrestoVersion(String prestoVersion)
    {
        this.prestoVersion = prestoVersion;
        return this;
    }

    @Deprecated
    public String getDataSources()
    {
        return dataSources;
    }

    @Deprecated
    @Config("datasources")
    public ServerConfig setDataSources(String dataSources)
    {
        this.dataSources = dataSources;
        return this;
    }

    public boolean isIncludeExceptionInResponse()
    {
        return includeExceptionInResponse;
    }

    @Config("http.include-exception-in-response")
    public ServerConfig setIncludeExceptionInResponse(boolean includeExceptionInResponse)
    {
        this.includeExceptionInResponse = includeExceptionInResponse;
        return this;
    }

    public Duration getGracePeriod()
    {
        return gracePeriod;
    }

    @Config("shutdown.grace-period")
    public ServerConfig setGracePeriod(Duration gracePeriod)
    {
        this.gracePeriod = gracePeriod;
        return this;
    }
}

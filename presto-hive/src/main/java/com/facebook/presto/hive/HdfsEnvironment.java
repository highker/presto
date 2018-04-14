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

import com.facebook.presto.hadoop.HadoopFileSystemCache;
import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.authentication.GenericExceptionAction;
import com.facebook.presto.hive.authentication.HdfsAuthentication;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.security.BasicPrincipal;
import com.facebook.presto.spi.security.Identity;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.airlift.json.ObjectMapperProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

import java.io.IOException;
import java.security.Principal;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HdfsEnvironment
{
    static {
        HadoopNative.requireHadoopNative();
        HadoopFileSystemCache.initialize();
    }

    private final HdfsConfiguration hdfsConfiguration;
    private final HdfsAuthentication hdfsAuthentication;
    private final boolean verifyChecksum;

    @Inject
    public HdfsEnvironment(
            HdfsConfiguration hdfsConfiguration,
            HiveClientConfig config,
            HdfsAuthentication hdfsAuthentication)
    {
        this.hdfsConfiguration = requireNonNull(hdfsConfiguration, "hdfsConfiguration is null");
        this.verifyChecksum = requireNonNull(config, "config is null").isVerifyChecksum();
        this.hdfsAuthentication = requireNonNull(hdfsAuthentication, "hdfsAuthentication is null");
    }

    public Configuration getConfiguration(HdfsContext context, Path path)
    {
        return hdfsConfiguration.getConfiguration(context, path.toUri());
    }

    public FileSystem getFileSystem(HdfsContext context, Path path)
            throws IOException
    {
        return getFileSystem(context.getIdentity().getUser(), path, getConfiguration(context, path));
    }

    public FileSystem getFileSystem(String user, Path path, Configuration configuration)
            throws IOException
    {
        return hdfsAuthentication.doAs(user, () -> {
            FileSystem fileSystem = path.getFileSystem(configuration);
            fileSystem.setVerifyChecksum(verifyChecksum);
            return fileSystem;
        });
    }

    public <R, E extends Exception> R doAs(String user, GenericExceptionAction<R, E> action)
            throws E
    {
        return hdfsAuthentication.doAs(user, action);
    }

    public void doAs(String user, Runnable action)
    {
        hdfsAuthentication.doAs(user, action);
    }

    @Immutable
    @JsonDeserialize(using = HdfsEnvironment.HdfsContext.HdfsContextDeserializer.class)
    public static class HdfsContext
    {
        private final Identity identity;
        private final Optional<String> source;
        private final Optional<String> queryId;
        private final Optional<String> schemaName;
        private final Optional<String> tableName;
        private final Optional<String> principalClassName;

        public HdfsContext(Identity identity)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.source = Optional.empty();
            this.queryId = Optional.empty();
            this.schemaName = Optional.empty();
            this.tableName = Optional.empty();
            this.principalClassName = getPrincipalClassName(identity);
        }

        public HdfsContext(ConnectorSession session, String schemaName)
        {
            requireNonNull(session, "session is null");
            requireNonNull(schemaName, "schemaName is null");
            this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
            this.source = requireNonNull(session.getSource(), "session.getSource()");
            this.queryId = Optional.of(session.getQueryId());
            this.schemaName = Optional.of(schemaName);
            this.tableName = Optional.empty();
            this.principalClassName = getPrincipalClassName(identity);
        }

        public HdfsContext(ConnectorSession session, String schemaName, String tableName)
        {
            requireNonNull(session, "session is null");
            requireNonNull(schemaName, "schemaName is null");
            requireNonNull(tableName, "tableName is null");
            this.identity = requireNonNull(session.getIdentity(), "session.getIdentity() is null");
            this.source = requireNonNull(session.getSource(), "session.getSource()");
            this.queryId = Optional.of(session.getQueryId());
            this.schemaName = Optional.of(schemaName);
            this.tableName = Optional.of(tableName);
            this.principalClassName = getPrincipalClassName(identity);
        }

        private HdfsContext(
                Identity identity,
                Optional<String> source,
                Optional<String> queryId,
                Optional<String> schemaName,
                Optional<String> tableName,
                Optional<String> principalClassName)
        {
            this.identity = requireNonNull(identity, "identity is null");
            this.source = requireNonNull(source, "source is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.schemaName = requireNonNull(schemaName, "schemaName is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.principalClassName = requireNonNull(principalClassName, "principalClassName is null");
        }

        @JsonProperty
        public Identity getIdentity()
        {
            return identity;
        }

        @JsonProperty
        public Optional<String> getSource()
        {
            return source;
        }

        @JsonProperty
        public Optional<String> getQueryId()
        {
            return queryId;
        }

        @JsonProperty
        public Optional<String> getSchemaName()
        {
            return schemaName;
        }

        @JsonProperty
        public Optional<String> getTableName()
        {
            return tableName;
        }

        @JsonProperty
        public Optional<String> getPrincipalClassName()
        {
            return principalClassName;
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
            HdfsContext otherHdfsContext = (HdfsContext) other;
            return Objects.equals(identity, otherHdfsContext.identity) &&
                    Objects.equals(source, otherHdfsContext.source) &&
                    Objects.equals(queryId, otherHdfsContext.queryId) &&
                    Objects.equals(schemaName, otherHdfsContext.schemaName) &&
                    Objects.equals(tableName, otherHdfsContext.tableName) &&
                    Objects.equals(principalClassName, otherHdfsContext.principalClassName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(identity, source, queryId, schemaName, tableName, principalClassName);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .omitNullValues()
                    .add("user", identity)
                    .add("source", source.orElse(null))
                    .add("queryId", queryId.orElse(null))
                    .add("schemaName", schemaName.orElse(null))
                    .add("tableName", tableName.orElse(null))
                    .toString();
        }

        public static class HdfsContextDeserializer
                extends JsonDeserializer<HdfsContext>
        {
            private static final ObjectMapper MAPPER = new ObjectMapperProvider().get();

            @Override
            public HdfsContext deserialize(JsonParser jsonParser, DeserializationContext context)
                    throws IOException
            {
                JsonNode node = jsonParser.getCodec().readTree(jsonParser);

                Optional<String> source = MAPPER.readValue(MAPPER.treeAsTokens(node.get("source")), new TypeReference<Optional<String>>() {});
                Optional<String> queryId = MAPPER.readValue(MAPPER.treeAsTokens(node.get("queryId")), new TypeReference<Optional<String>>() {});
                Optional<String> schemaName = MAPPER.readValue(MAPPER.treeAsTokens(node.get("schemaName")), new TypeReference<Optional<String>>() {});
                Optional<String> tableName = MAPPER.readValue(MAPPER.treeAsTokens(node.get("tableName")), new TypeReference<Optional<String>>() {});
                Optional<String> principalClassName = MAPPER.readValue(MAPPER.treeAsTokens(node.get("principalClassName")), new TypeReference<Optional<String>>() {});

                String user = MAPPER.readValue(MAPPER.treeAsTokens(node.get("identity").get("user")), String.class);
                Optional<Principal> principal;
                if (!principalClassName.isPresent()) {
                    principal = Optional.empty();
                }
                else if (principalClassName.get().equals(BasicPrincipal.class.getSimpleName())){
                    principal = MAPPER.readValue( MAPPER.treeAsTokens(node.get("identity").get("principal")), new TypeReference<Optional<BasicPrincipal>>() {});
                }
                else {
                    throw new UnsupportedOperationException(format("Unsupported principal [%s]", principalClassName.get()));
                }

                return new HdfsContext(new Identity(user, principal), source, queryId, schemaName, tableName, principalClassName);
            }
        }

        private static Optional<String> getPrincipalClassName(Identity identity) {
            return identity.getPrincipal().map(principal -> principal.getClass().getSimpleName());
        }
    }
}

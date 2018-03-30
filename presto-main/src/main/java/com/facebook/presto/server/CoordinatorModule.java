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

import com.facebook.presto.client.QueryResults;
import com.facebook.presto.execution.AddColumnTask;
import com.facebook.presto.execution.CallTask;
import com.facebook.presto.execution.CommitTask;
import com.facebook.presto.execution.CreateSchemaTask;
import com.facebook.presto.execution.CreateTableTask;
import com.facebook.presto.execution.CreateViewTask;
import com.facebook.presto.execution.DataDefinitionTask;
import com.facebook.presto.execution.DeallocateTask;
import com.facebook.presto.execution.DispatchManager;
import com.facebook.presto.execution.DispatchQueryExecution.DispatchQueryExecutionFactory;
import com.facebook.presto.execution.DropColumnTask;
import com.facebook.presto.execution.DropSchemaTask;
import com.facebook.presto.execution.DropTableTask;
import com.facebook.presto.execution.DropViewTask;
import com.facebook.presto.execution.ForDispatchTransaction;
import com.facebook.presto.execution.ForQueryExecution;
import com.facebook.presto.execution.GrantTask;
import com.facebook.presto.execution.PrepareTask;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryExecutionMBean;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryQueueManager;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.RenameColumnTask;
import com.facebook.presto.execution.RenameSchemaTask;
import com.facebook.presto.execution.RenameTableTask;
import com.facebook.presto.execution.ResetSessionTask;
import com.facebook.presto.execution.RevokeTask;
import com.facebook.presto.execution.RollbackTask;
import com.facebook.presto.execution.SetSessionTask;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.execution.StartTransactionTask;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.UseTask;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.LegacyResourceGroupConfigurationManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.execution.scheduler.AllAtOnceExecutionPolicy;
import com.facebook.presto.execution.scheduler.ExecutionPolicy;
import com.facebook.presto.execution.scheduler.PhasedExecutionPolicy;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.ForMemoryManager;
import com.facebook.presto.memory.LowMemoryKiller;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.MemoryManagerConfig.LowMemoryKillerPolicy;
import com.facebook.presto.memory.NoneLowMemoryKiller;
import com.facebook.presto.memory.TotalReservationLowMemoryKiller;
import com.facebook.presto.memory.TotalReservationOnBlockedNodesLowMemoryKiller;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.server.protocol.DispatchQuery.DispatchQueryFactory;
import com.facebook.presto.server.protocol.Query.QueryFactory;
import com.facebook.presto.server.protocol.SqlQuery.SqlQueryFactory;
import com.facebook.presto.server.protocol.StatementResource;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.tree.AddColumn;
import com.facebook.presto.sql.tree.Call;
import com.facebook.presto.sql.tree.Commit;
import com.facebook.presto.sql.tree.CreateSchema;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.Deallocate;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.DropColumn;
import com.facebook.presto.sql.tree.DropSchema;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Grant;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Prepare;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.RenameColumn;
import com.facebook.presto.sql.tree.RenameSchema;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.Revoke;
import com.facebook.presto.sql.tree.Rollback;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowCreate;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowGrants;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowStats;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.StartTransaction;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Use;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.execution.DataDefinitionExecution.DataDefinitionExecutionFactory;
import static com.facebook.presto.execution.QueryExecution.QueryExecutionFactory;
import static com.facebook.presto.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import static com.facebook.presto.spi.NodeType.isCoordinator;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.configuration.ConditionalModule.installModuleIf;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.http.server.HttpServerBinder.httpServerBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class CoordinatorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);
        boolean isCoordinator = isCoordinator(serverConfig.getNodeType());

        httpServerBinder(binder).bindResource("/", "webapp").withWelcomeFile("index.html");

        // presto coordinator announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto-coordinator");

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(QueryResults.class);
        jaxrsBinder(binder).bind(StatementResource.class);

        // query execution visualizer
        jaxrsBinder(binder).bind(QueryExecutionResource.class);

        // query manager
        jaxrsBinder(binder).bind(QueryResource.class);
        jaxrsBinder(binder).bind(StageResource.class);
        jaxrsBinder(binder).bind(QueryStateInfoResource.class);
        jaxrsBinder(binder).bind(ResourceGroupStateInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);
        binder.bind(QueryManager.class).to(SqlQueryManager.class).in(Scopes.SINGLETON);
        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);
        binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
        binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
        binder.bind(QueryQueueManager.class).to(InternalResourceGroupManager.class);
        binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryManager.class).withGeneratedName();

        // cluster memory manager
        binder.bind(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        binder.bind(ClusterMemoryPoolManager.class).to(ClusterMemoryManager.class).in(Scopes.SINGLETON);
        httpClientBinder(binder).bindHttpClient("memoryManager", ForMemoryManager.class)
                .withTracing()
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                });
        bindLowMemoryKiller(LowMemoryKillerPolicy.NONE, NoneLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION, TotalReservationLowMemoryKiller.class);
        bindLowMemoryKiller(LowMemoryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesLowMemoryKiller.class);
        newExporter(binder).export(ClusterMemoryManager.class).withGeneratedName();

        // cluster statistics
        jaxrsBinder(binder).bind(ClusterStatsResource.class);

        // query explainer
        binder.bind(QueryExplainer.class).in(Scopes.SINGLETON);

        // execution scheduler
        binder.bind(RemoteTaskFactory.class).to(HttpRemoteTaskFactory.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskFactory.class).withGeneratedName();

        binder.bind(RemoteTaskStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(RemoteTaskStats.class).withGeneratedName();

        httpClientBinder(binder).bindHttpClient("scheduler", ForScheduler.class)
                .withTracing()
                .withFilter(GenerateTraceTokenRequestFilter.class)
                .withConfigDefaults(config -> {
                    config.setIdleTimeout(new Duration(30, SECONDS));
                    config.setRequestTimeout(new Duration(10, SECONDS));
                    config.setMaxConnectionsPerServer(250);
                });

        binder.bind(ScheduledExecutorService.class).annotatedWith(ForScheduler.class)
                .toInstance(newSingleThreadScheduledExecutor(threadsNamed("stage-scheduler")));

        // query execution
        binder.bind(ExecutorService.class).annotatedWith(ForQueryExecution.class)
                .toInstance(newCachedThreadPool(threadsNamed("query-execution-%s")));
        binder.bind(QueryExecutionMBean.class).in(Scopes.SINGLETON);
        newExporter(binder).export(QueryExecutionMBean.class).as(generatedNameOf(QueryExecution.class));

        MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryExecutionFactory<?>>() {});

        MapBinder<Class<? extends Statement>, QueryFactory<?>> queryBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryFactory<?>>() {});

        binder.bind(SplitSchedulerStats.class).in(Scopes.SINGLETON);
        newExporter(binder).export(SplitSchedulerStats.class).withGeneratedName();

        if (isCoordinator(serverConfig.getNodeType())) {
            bindSqlFactory(binder, SqlQueryExecutionFactory.class, executionBinder);
            bindSqlFactory(binder, SqlQueryFactory.class, queryBinder);
        }
        else {
            httpClientBinder(binder).bindHttpClient("dispatchTransactionManager", ForDispatchTransaction.class)
                    .withTracing()
                    .withConfigDefaults(config -> {
                        config.setIdleTimeout(new Duration(5, SECONDS));
                        config.setRequestTimeout(new Duration(10, SECONDS));
                    });
            binder.bind(DispatchManager.class).in(Scopes.SINGLETON);

            binder.bind(ScheduledExecutorService.class).annotatedWith(ForQueryExecution.class)
                    .toInstance(newScheduledThreadPool(5, daemonThreadsNamed("dispatch-query-execution-%s")));

            httpClientBinder(binder).bindHttpClient("queryExecutionFactory", ForQueryExecution.class)
                    .withTracing()
                    .withConfigDefaults(config -> {
                        config.setIdleTimeout(new Duration(10, SECONDS));
                        config.setRequestTimeout(new Duration(20, SECONDS));
                    });
            bindSqlFactory(binder, DispatchQueryExecutionFactory.class, executionBinder);
            bindSqlFactory(binder, DispatchQueryFactory.class, queryBinder);
        }

        binder.bind(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, CreateSchema.class, CreateSchemaTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, DropSchema.class, DropSchemaTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, RenameSchema.class, RenameSchemaTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, AddColumn.class, AddColumnTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, CreateTable.class, CreateTableTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, RenameTable.class, RenameTableTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, RenameColumn.class, RenameColumnTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, DropColumn.class, DropColumnTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, DropTable.class, DropTableTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, CreateView.class, CreateViewTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, DropView.class, DropViewTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, Use.class, UseTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, SetSession.class, SetSessionTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, ResetSession.class, ResetSessionTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, StartTransaction.class, StartTransactionTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, Commit.class, CommitTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, Rollback.class, RollbackTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, Call.class, CallTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, Grant.class, GrantTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, Revoke.class, RevokeTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, Prepare.class, PrepareTask.class);
        bindDataDefinitionTask(isCoordinator, binder, executionBinder, queryBinder, Deallocate.class, DeallocateTask.class);

        MapBinder<String, ExecutionPolicy> executionPolicyBinder = newMapBinder(binder, String.class, ExecutionPolicy.class);
        executionPolicyBinder.addBinding("all-at-once").to(AllAtOnceExecutionPolicy.class);
        executionPolicyBinder.addBinding("phased").to(PhasedExecutionPolicy.class);

        // cleanup
        binder.bind(ExecutorCleanup.class).in(Scopes.SINGLETON);
    }

    private static <T extends Statement> void bindDataDefinitionTask(
            boolean isCoordinator,
            Binder binder,
            MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder,
            MapBinder<Class<? extends Statement>, QueryFactory<?>> queryBinder,
            Class<T> statement,
            Class<? extends DataDefinitionTask<T>> task)
    {
        MapBinder<Class<? extends Statement>, DataDefinitionTask<?>> taskBinder = newMapBinder(binder,
                new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<DataDefinitionTask<?>>() {});

        taskBinder.addBinding(statement).to(task).in(Scopes.SINGLETON);
        if (isCoordinator) {
            // execution on the current server
            executionBinder.addBinding(statement).to(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
            queryBinder.addBinding(statement).to(SqlQueryFactory.class).in(Scopes.SINGLETON);
        }
        else {
            executionBinder.addBinding(statement).to(DispatchQueryExecutionFactory.class).in(Scopes.SINGLETON);
            queryBinder.addBinding(statement).to(DispatchQueryFactory.class).in(Scopes.SINGLETON);
        }
    }

    private static <T> void bindSqlFactory(Binder binder, Class<? extends T> factory, MapBinder<Class<? extends Statement>, T> factoryMapBinder)
    {
        binder.bind(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(Query.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(Explain.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowCreate.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowColumns.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowStats.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowPartitions.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowFunctions.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowTables.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowSchemas.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowCatalogs.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowSession.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(ShowGrants.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(CreateTableAsSelect.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(Insert.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(Delete.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(DescribeInput.class).to(factory).in(Scopes.SINGLETON);
        factoryMapBinder.addBinding(DescribeOutput.class).to(factory).in(Scopes.SINGLETON);
    }

    private void bindLowMemoryKiller(String name, Class<? extends LowMemoryKiller> clazz)
    {
        install(installModuleIf(
                MemoryManagerConfig.class,
                config -> name.equals(config.getLowMemoryKillerPolicy()),
                binder -> binder.bind(LowMemoryKiller.class).to(clazz).in(Scopes.SINGLETON)));
    }

    public static class ExecutorCleanup
    {
        private final List<ExecutorService> executors;

        @Inject
        public ExecutorCleanup(
                @ForQueryExecution ExecutorService queryExecutionExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor)
        {
            executors = ImmutableList.<ExecutorService>builder()
                    .add(queryExecutionExecutor)
                    .add(schedulerExecutor)
                    .build();
        }

        @PreDestroy
        public void shutdown()
        {
            executors.forEach(ExecutorService::shutdownNow);
        }
    }
}

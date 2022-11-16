/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kyligence.kap.clickhouse;

import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_INDEX_CLEAN_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_JOB_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_MODEL_CLEAN_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_NODE_CLEAN_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_REFRESH_SECONDARY_INDEXES_FACTORY;
import static org.apache.kylin.job.factory.JobFactoryConstant.STORAGE_SEGMENT_CLEAN_FACTORY;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.ClickHouseConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageStepFactory;
import org.apache.kylin.job.factory.JobFactory;
import org.apache.spark.sql.execution.datasources.jdbc.ClickHouseDialect$;
import org.apache.spark.sql.jdbc.JdbcDialects;

import io.kyligence.kap.clickhouse.factory.ClickHouseMetadataFactory;
import io.kyligence.kap.clickhouse.factory.ClickHouseOperatorFactory;
import io.kyligence.kap.clickhouse.factory.ClickHouseQueryFactory;
import io.kyligence.kap.clickhouse.factory.ClickhouseIndexFactory;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseIndexClean;
import io.kyligence.kap.clickhouse.job.ClickHouseIndexCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseJob;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.clickhouse.job.ClickHouseMerge;
import io.kyligence.kap.clickhouse.job.ClickHouseModelCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseProjectCleanJob;
import io.kyligence.kap.clickhouse.job.ClickHouseRefresh;
import io.kyligence.kap.clickhouse.job.ClickHouseRefreshSecondaryIndexJob;
import io.kyligence.kap.clickhouse.job.ClickHouseSegmentCleanJob;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.clickhouse.metadata.ClickHouseFlowManager;
import io.kyligence.kap.clickhouse.metadata.ClickHouseManager;
import io.kyligence.kap.clickhouse.metadata.ClickHouseNodeGroupManager;
import io.kyligence.kap.guava20.shaded.common.base.Strings;
import io.kyligence.kap.secondstorage.SecondStorageConfigLoader;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStoragePlugin;
import io.kyligence.kap.secondstorage.config.ClusterInfo;
import io.kyligence.kap.secondstorage.config.Node;
import io.kyligence.kap.secondstorage.factory.SecondStorageDatabaseOperatorFactory;
import io.kyligence.kap.secondstorage.factory.SecondStorageFactoryUtils;
import io.kyligence.kap.secondstorage.factory.SecondStorageIndexFactory;
import io.kyligence.kap.secondstorage.factory.SecondStorageMetadataFactory;
import io.kyligence.kap.secondstorage.factory.SecondStorageQueryOperatorFactory;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;

public class ClickHouseStorage implements SecondStoragePlugin {

    public ClickHouseStorage() {
        reloadNodeMap();
    }

    /**
     * clean node mapping cache
     */
    public static void reloadNodeMap() {
        ClickHouseConfigLoader.getInstance().refresh();
        ClusterInfo cluster = ClickHouseConfigLoader.getInstance().getCluster();
        SecondStorageNodeHelper.clear();
        SecondStorageNodeHelper.initFromCluster(
                cluster,
                node -> ClickHouse.buildUrl(node.getIp(), node.getPort(), getJdbcUrlProperties(cluster, node)),
                (nodes, queryContext) -> {
                    if (nodes.isEmpty()) {
                        return "";
                    }

                    StringBuilder sb = new StringBuilder();
                    for (Node node : nodes) {
                        if (Strings.isNullOrEmpty(sb.toString())) {
                            sb.append(node.getIp()).append(":").append(node.getPort());
                        } else {
                            sb.append(",").append(node.getIp()).append(":").append(node.getPort());
                        }
                    }

                    String clientName;
                    if (CollectionUtils.isEmpty(queryContext.getSecondStorageUrls())) {
                        clientName = queryContext.getQueryId() + "_1";
                    } else {
                        clientName = queryContext.getQueryId() + "_2";
                    }

                    Map<String, String> params = getJdbcUrlProperties(cluster, nodes.get(0));
                    params.put(ClickHouse.CLIENT_NAME, clientName);

                    return ClickHouse.buildUrl(sb.toString(), params);
                });
    }

    @Override
    public boolean ready() {
        ClickHouseConfig config = ClickHouseConfig.getInstanceFromEnv();
        return StringUtils.isNotEmpty(config.getClusterConfig());
    }

    @Override
    public String queryCatalog() {
        ClickHouseConfig config = ClickHouseConfig.getInstanceFromEnv();
        return config.getQueryCatalog();
    }

    @Override
    public Manager<TableFlow> tableFlowManager(KylinConfig config, String project) {
        return config.getManager(project, ClickHouseFlowManager.class);
    }

    @Override
    public Manager<TablePlan> tablePlanManager(KylinConfig config, String project) {
        return config.getManager(project, ClickHouseManager.class);
    }

    @Override
    public Manager<NodeGroup> nodeGroupManager(KylinConfig config, String project) {
        return config.getManager(project, ClickHouseNodeGroupManager.class);
    }

    @Override
    public SecondStorageConfigLoader getConfigLoader() {
        return ClickHouseConfigLoader.getInstance();
    }

    static {
        JdbcDialects.registerDialect(ClickHouseDialect$.MODULE$);
        JobFactory.register(STORAGE_JOB_FACTORY, new ClickHouseJob.StorageJobFactory());
        JobFactory.register(STORAGE_MODEL_CLEAN_FACTORY, new ClickHouseModelCleanJob.ModelCleanJobFactory());
        JobFactory.register(STORAGE_NODE_CLEAN_FACTORY, new ClickHouseProjectCleanJob.ProjectCleanJobFactory());
        JobFactory.register(STORAGE_SEGMENT_CLEAN_FACTORY, new ClickHouseSegmentCleanJob.SegmentCleanJobFactory());
        JobFactory.register(STORAGE_INDEX_CLEAN_FACTORY, new ClickHouseIndexCleanJob.IndexCleanJobFactory());
        JobFactory.register(STORAGE_REFRESH_SECONDARY_INDEXES_FACTORY,
                new ClickHouseRefreshSecondaryIndexJob.RefreshSecondaryIndexJobFactory());

        SecondStorageStepFactory.register(SecondStorageStepFactory.SecondStorageLoadStep.class, ClickHouseLoad::new);
        SecondStorageStepFactory.register(SecondStorageStepFactory.SecondStorageRefreshStep.class, ClickHouseRefresh::new);
        SecondStorageStepFactory.register(SecondStorageStepFactory.SecondStorageMergeStep.class, ClickHouseMerge::new);
        SecondStorageStepFactory.register(SecondStorageStepFactory.SecondStorageIndexClean.class, ClickHouseIndexClean::new);

        SecondStorageFactoryUtils.register(SecondStorageMetadataFactory.class, new ClickHouseMetadataFactory());
        SecondStorageFactoryUtils.register(SecondStorageDatabaseOperatorFactory.class, new ClickHouseOperatorFactory());
        SecondStorageFactoryUtils.register(SecondStorageQueryOperatorFactory.class, new ClickHouseQueryFactory());
        SecondStorageFactoryUtils.register(SecondStorageIndexFactory.class, new ClickhouseIndexFactory());
    }

    public static Map<String, String> getJdbcUrlProperties(ClusterInfo cluster, Node node) {

        Map<String, String> param = new HashMap<>(4);

        if (StringUtils.isNotEmpty(cluster.getKeepAliveTimeout())) {
            param.put(ClickHouse.KEEP_ALIVE_TIMEOUT, cluster.getKeepAliveTimeout());
        }
        if (StringUtils.isNotEmpty(cluster.getSocketTimeout())) {
            param.put(ClickHouse.SOCKET_TIMEOUT, cluster.getSocketTimeout());
        }
        if (StringUtils.isNotEmpty(node.getUser())) {
            param.put(ClickHouse.USER, node.getUser());
        }
        if (StringUtils.isNotEmpty(node.getPassword())) {
            param.put(ClickHouse.PASSWORD, node.getPassword());
        }

        return param;
    }
}
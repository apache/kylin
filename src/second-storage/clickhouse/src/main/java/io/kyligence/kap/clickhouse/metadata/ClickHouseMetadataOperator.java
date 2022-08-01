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
package io.kyligence.kap.clickhouse.metadata;

import com.google.common.base.Preconditions;
import io.kyligence.kap.clickhouse.job.ClickHouse;
import io.kyligence.kap.clickhouse.job.ClickHouseTableStorageMetric;
import io.kyligence.kap.clickhouse.parser.ExistsQueryParser;
import io.kyligence.kap.clickhouse.parser.ShowCreateQueryParser;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageQueryRouteUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.config.ConfigOption;
import io.kyligence.kap.secondstorage.config.DefaultSecondStorageProperties;
import io.kyligence.kap.secondstorage.config.SecondStorageModelSegment;
import io.kyligence.kap.secondstorage.config.SecondStorageProjectModelSegment;
import io.kyligence.kap.secondstorage.config.SecondStorageProperties;
import io.kyligence.kap.secondstorage.config.SecondStorageSegment;
import io.kyligence.kap.secondstorage.ddl.ExistsDatabase;
import io.kyligence.kap.secondstorage.ddl.ExistsTable;
import io.kyligence.kap.secondstorage.ddl.ShowCreateDatabase;
import io.kyligence.kap.secondstorage.ddl.ShowCreateTable;
import io.kyligence.kap.secondstorage.ddl.exp.TableIdentifier;
import io.kyligence.kap.secondstorage.metadata.MetadataOperator;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.SegmentFileStatus;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import io.kyligence.kap.secondstorage.response.SizeInNodeResponse;
import io.kyligence.kap.secondstorage.response.TableSyncResponse;
import io.kyligence.kap.secondstorage.util.SecondStorageSqlUtils;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseMetadataOperator implements MetadataOperator {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseMetadataOperator.class);

    private final SecondStorageProperties properties;

    public ClickHouseMetadataOperator(SecondStorageProperties properties) {
        this.properties = new DefaultSecondStorageProperties(properties.getProperties());
    }

    @Override
    public TableSyncResponse tableSync() {
        String project = properties.get(new ConfigOption<>(SecondStorageConstants.PROJECT, String.class));

        SecondStorageUtil.checkSecondStorageData(project);
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        List<NodeGroup> nodeGroups = SecondStorageUtil.listNodeGroup(config, project);
        Set<String> nodes = nodeGroups.stream()
                .flatMap(x -> x.getNodeNames().stream())
                .collect(Collectors.toSet());

        List<TableFlow> tableFlows = SecondStorageUtil.listTableFlow(config, project);
        tableFlows = tableFlows.stream()
                .filter(x -> x.getTableDataList() != null && x.getTableDataList().size() > 0)
                .collect(Collectors.toList());

        if (tableFlows.isEmpty()) {
            logger.warn("project={} second storage data is empty", project);
            return new TableSyncResponse(project);
        }

        //one project one database
        String database = tableFlows.get(0).getTableDataList().get(0).getDatabase();

        Set<String> tables = tableFlows.stream()
                .flatMap(x -> x.getTableDataList().stream())
                .map(TableData::getTable)
                .collect(Collectors.toSet());

        Map<String, String> tableCreateSqlMap = new HashMap<>();

        String databaseCreateSql = null;
        for (String node : nodes) {
            if (!SecondStorageQueryRouteUtil.getNodeStatus(node)) {
                continue;
            }

            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                if (databaseCreateSql == null) {
                    int existCode = clickHouse.query(new ExistsDatabase(database).toSql(), ExistsQueryParser.EXISTS).get(0);
                    if (existCode == 1) {
                        databaseCreateSql = clickHouse.query(new ShowCreateDatabase(database).toSql(), ShowCreateQueryParser.SHOW_CREATE).get(0);
                    }
                }
                for (String table : tables) {
                    if (tableCreateSqlMap.get(table) == null) {
                        int existCode = clickHouse.query(new ExistsTable(TableIdentifier.table(database, table)).toSql(), ExistsQueryParser.EXISTS).get(0);
                        if (existCode == 1) {
                            tableCreateSqlMap.put(table,
                                    SecondStorageSqlUtils.addIfNotExists(clickHouse.query(new ShowCreateTable(TableIdentifier.table(database, table)).toSql(),
                                            ShowCreateQueryParser.SHOW_CREATE).get(0), "TABLE")
                            );
                        }
                    }
                }
            } catch (SQLException sqlException) {
                ExceptionUtils.rethrow(sqlException);
            }
        }
        databaseCreateSql = SecondStorageSqlUtils.addIfNotExists(databaseCreateSql, "DATABASE");
        for (String node : nodes) {
            if (!SecondStorageQueryRouteUtil.getNodeStatus(node)) {
                continue;
            }

            try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                clickHouse.apply(databaseCreateSql);
                for (String sql : tableCreateSqlMap.values()) {
                    clickHouse.apply(sql);
                }
            } catch (SQLException sqlException) {
                ExceptionUtils.rethrow(sqlException);
            }
        }
        return new TableSyncResponse(project, new ArrayList<>(nodes), database, new ArrayList<>(tables));
    }

    private NodeGroup getNodeGroup(List<NodeGroup> nodeGroups, Set<String> existShardNodes){
        Preconditions.checkArgument(!nodeGroups.isEmpty());
        val existShardNodesList = new ArrayList<>(existShardNodes);
        NodeGroup addGroup = nodeGroups.get(0);
        if (existShardNodesList.size() > 0) {
            for (NodeGroup nodeGroup : nodeGroups){
                val nodeNames = nodeGroup.getNodeNames();
                val item = existShardNodesList.get(0);
                if (nodeNames.contains(item)){
                    addGroup = nodeGroup;
                    break;
                }
            }
        }
        return addGroup;
    }

    @Override
    public SizeInNodeResponse sizeInNode() {
        SecondStorageProjectModelSegment projectModelSegment = properties.get(new ConfigOption<>(SecondStorageConstants.PROJECT_MODEL_SEGMENT_PARAM, SecondStorageProjectModelSegment.class));
        String project = projectModelSegment.getProject();
        Map<String, SecondStorageModelSegment> modelSegmentMap = projectModelSegment.getModelSegmentMap();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        SecondStorageUtil.checkSecondStorageData(project);
        List<TableFlow> tableFlows = SecondStorageUtil.listTableFlow(config, project);

        List<NodeGroup> nodeGroups = SecondStorageUtil.listNodeGroup(config, project);
        Set<String> nodes = nodeGroups.stream()
                .flatMap(x -> x.getNodeNames().stream())
                .collect(Collectors.toSet());

        ClickHouseTableStorageMetric storageMetric = new ClickHouseTableStorageMetric(new ArrayList<>(nodes));
        storageMetric.collect(true);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            tableFlows.forEach(tableFlow -> {
                tableFlow.update(copied -> {
                    copied.getTableDataList().forEach(tableData -> {
                        List<TablePartition> tablePartitions = tableData.getPartitions();
                        val newTablePartitions = new ArrayList<TablePartition>();
                        for (TablePartition tablePartition : tablePartitions) {
                            SecondStorageModelSegment modelSegment = modelSegmentMap.get(tableFlow.getUuid());
                            SecondStorageSegment secondStorageSegment = modelSegment.getSegmentMap().get(tablePartition.getSegmentId());
                            Map<String, Long> sizeInNodeMap = storageMetric.getByPartitions(tableData.getDatabase(), tableData.getTable(), secondStorageSegment.getSegmentRange(), modelSegment.getDateFormat());
                            Set<String> existShardNodes = new HashSet<>(tablePartition.getShardNodes());
                            NodeGroup addGroup = getNodeGroup(nodeGroups, existShardNodes);
                            List<String> addShardNodes = addGroup.getNodeNames().stream()
                                    .filter(node -> !existShardNodes.contains(node))
                                    .collect(Collectors.toList());

                            tablePartition.getSizeInNode().entrySet().forEach(
                                    e -> e.setValue(sizeInNodeMap.getOrDefault(e.getKey(), 0L))
                            );

                            List<String> shardNodes = new ArrayList<>(tablePartition.getShardNodes());
                            shardNodes.addAll(addShardNodes);

                            Map<String, Long> sizeInNode = new HashMap<>(tablePartition.getSizeInNode());

                            sizeInNode.entrySet().forEach(
                                    e -> e.setValue(sizeInNodeMap.getOrDefault(e.getKey(), 0L))
                            );

                            Map<String, List<SegmentFileStatus>> nodeFileMap = new HashMap<>(tablePartition.getNodeFileMap());

                            for (String node : addShardNodes) {
                                sizeInNode.put(node, sizeInNodeMap.getOrDefault(node, 0L));
                                nodeFileMap.put(node, new ArrayList<>());
                            }

                            TablePartition.Builder builder = new TablePartition.Builder();
                            builder.setId(tablePartition.getId())
                                    .setSegmentId(tablePartition.getSegmentId())
                                    .setShardNodes(shardNodes)
                                    .setSizeInNode(sizeInNode)
                                    .setNodeFileMap(nodeFileMap);
                            newTablePartitions.add(builder.build());
                        }
                        newTablePartitions.forEach(tableData::addPartition);
                    });
                });
            });
            return null;
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
        return new SizeInNodeResponse(project);
    }
}

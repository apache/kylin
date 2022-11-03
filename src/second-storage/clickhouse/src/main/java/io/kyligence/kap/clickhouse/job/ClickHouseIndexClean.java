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

package io.kyligence.kap.clickhouse.job;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_INDEX_CLEAN;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;

import com.clearspring.analytics.util.Preconditions;

import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseIndexClean extends AbstractClickHouseClean {
    private Set<Long> needDeleteLayoutIds;

    private String dateFormat;

    private Map<String, SegmentRange<Long>> segmentRangeMap;

    // can't delete because reflect
    public ClickHouseIndexClean() {
        setName(STEP_SECOND_STORAGE_INDEX_CLEAN);
    }

    // can't delete because reflect
    public ClickHouseIndexClean(Object notSetId) {
        super(notSetId);
    }

    public void setNeedDeleteLayoutIds(Set<Long> needDeleteLayoutIds) {
        this.needDeleteLayoutIds = needDeleteLayoutIds;
    }

    public Set<Long> getNeedDeleteLayoutIds() {
        if (CollectionUtils.isNotEmpty(needDeleteLayoutIds)) {
            return this.needDeleteLayoutIds;
        }

        Set<Long> deleteLayoutIds = new HashSet<>();
        Optional.ofNullable(getExecutableManager(getProject()).getJob(getParentId()).getParams()).ifPresent(params -> {
            String toBeDeletedLayoutIdsStr = params.get(NBatchConstants.P_TO_BE_DELETED_LAYOUT_IDS);
            if (StringUtils.isNotBlank(toBeDeletedLayoutIdsStr)) {
                for (String id : toBeDeletedLayoutIdsStr.split(",")) {
                    deleteLayoutIds.add(Long.parseLong(id));
                }
            }
        });

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            SecondStorageUtil.tableFlowManager(getConfig(), getProject()).ifPresent(manage -> manage.update(
                    getTargetSubject(),
                    updater -> updater.cleanTableData(tableData -> deleteLayoutIds.contains(tableData.getLayoutID()))));
            SecondStorageUtil.tablePlanManager(getConfig(), getProject()).ifPresent(
                    manage -> manage.update(getTargetSubject(), updater -> updater.cleanTable(deleteLayoutIds)));
            return null;
        }, project, 1, getEpochId());

        return deleteLayoutIds;
    }

    public ClickHouseIndexClean setSegmentRangeMap(Map<String, SegmentRange<Long>> segmentRangeMap) {
        this.segmentRangeMap = segmentRangeMap;
        return this;
    }

    public void setDateFormat(final String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getDateFormat() {
        return this.dateFormat;
    }

    @Override
    public ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        return wrapWithExecuteException(() -> {
            if (INDEX_CLEAN_READY.equals(this.getParam(CLICKHOUSE_NODE_COUNT_PARAM))) {
                loadState();
            } else {
                internalInit();
            }
            workImpl();
            return ExecuteResult.createSucceed();
        });
    }

    @Override
    protected void internalInit() {
        KylinConfig config = getConfig();
        String modelId = getParam(NBatchConstants.P_DATAFLOW_ID);
        val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, getProject());
        val tableFlowManager = SecondStorageUtil.tableFlowManager(config, getProject());

        Preconditions.checkState(nodeGroupManager.isPresent() && tableFlowManager.isPresent());

        val tableFlow = tableFlowManager.get().get(modelId).orElse(null);
        if (tableFlow == null) {
            return;
        }

        setNodeCount(Math.toIntExact(nodeGroupManager.map(
                manager -> manager.listAll().stream().mapToLong(nodeGroup -> nodeGroup.getNodeNames().size()).sum())
                .orElse(0L)));
        List<String> nodes = nodeGroupManager.get().listAll()
                .stream()
                .flatMap(nodeGroup -> nodeGroup.getNodeNames().stream())
                .collect(Collectors.toList());
        getNeedDeleteLayoutIds().forEach(layoutId -> {
            // table_data not contains layout means deleted. Delete table instead partition
            if (segmentRangeMap == null || segmentRangeMap.isEmpty() || !tableFlow.getEntity(layoutId).isPresent()) {
                shardCleaners.addAll(cleanTable(nodes, layoutId));
            } else {
                segmentRangeMap.keySet()
                        .forEach(segmentId -> shardCleaners.addAll(cleanPartition(nodes, layoutId, segmentId)));
            }
        });
    }

    @Override
    protected Runnable getTask(ShardCleaner shardCleaner) {
        return () -> {
            try {
                if (shardCleaner.getPartitions() == null) {
                    shardCleaner.cleanTable();
                } else {
                    shardCleaner.cleanPartitions();
                }
            } catch (SQLException e) {
                log.error("node {} clean index {}.{} failed", shardCleaner.getClickHouse().getShardName(),
                        shardCleaner.getDatabase(), shardCleaner.getTable());
                ExceptionUtils.rethrow(e);
            }
        };
    }

    private List<ShardCleaner> cleanTable(List<String> nodes, long layoutId) {
        return nodes.stream().map(node ->
                new ShardCleaner(node, NameUtil.getDatabase(getConfig(), project),
                        NameUtil.getTable(getParam(NBatchConstants.P_DATAFLOW_ID), layoutId))
        ).collect(Collectors.toList());
    }

    private List<ShardCleaner> cleanPartition(List<String> nodes, long layoutId, String segmentId) {
        return nodes.stream().map(node ->
                new ShardCleaner(node, NameUtil.getDatabase(getConfig(), project), NameUtil.getTable(getParam(NBatchConstants.P_DATAFLOW_ID), layoutId),
                        SecondStorageDateUtils.splitByDay(segmentRangeMap.get(segmentId)), getDateFormat())
        ).collect(Collectors.toList());
    }
}

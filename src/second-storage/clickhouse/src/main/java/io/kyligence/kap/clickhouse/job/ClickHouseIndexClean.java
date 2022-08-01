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

import com.clearspring.analytics.util.Preconditions;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_INDEX_CLEAN;

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
        return this.needDeleteLayoutIds;
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
    protected void internalInit() {
        KylinConfig config = getConfig();
        String modelId = getParam(NBatchConstants.P_DATAFLOW_ID);
        val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, getProject());
        val tableFlowManager = SecondStorageUtil.tableFlowManager(config, getProject());

        Preconditions.checkState(nodeGroupManager.isPresent() && tableFlowManager.isPresent());

        val tableFlow = Objects.requireNonNull(tableFlowManager.get().get(modelId).orElse(null));

        setNodeCount(Math.toIntExact(nodeGroupManager.map(
                manager -> manager.listAll().stream().mapToLong(nodeGroup -> nodeGroup.getNodeNames().size()).sum())
                .orElse(0L)));


        List<String> nodes = nodeGroupManager.get().listAll()
                .stream()
                .flatMap(nodeGroup -> nodeGroup.getNodeNames().stream())
                .collect(Collectors.toList());


        getNeedDeleteLayoutIds().forEach(layoutId -> {
            // table_data not contains layout means deleted. Delete table instead partition
            if (segmentRangeMap.isEmpty() || !tableFlow.getEntity(layoutId).isPresent()) {
                shardCleaners.addAll(cleanTable(nodes, layoutId));
            } else {
                segmentRangeMap.keySet().forEach(segmentId -> shardCleaners.addAll(cleanPartition(nodes, layoutId, segmentId)));
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

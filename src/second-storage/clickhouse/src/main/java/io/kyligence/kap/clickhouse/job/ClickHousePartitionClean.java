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

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_SEGMENT_CLEAN;

import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;

import com.clearspring.analytics.util.Preconditions;

import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHousePartitionClean extends AbstractClickHouseClean {
    private String database;
    private String table;
    private Map<String, SegmentRange<Long>> segmentRangeMap;
    private String dateFormat;

    public ClickHousePartitionClean() {
        setName(STEP_SECOND_STORAGE_SEGMENT_CLEAN);
    }

    public ClickHousePartitionClean(Object notSetId) {
        super(notSetId);
    }

    public ClickHousePartitionClean setSegmentRangeMap(Map<String, SegmentRange<Long>> segmentRangeMap) {
        this.segmentRangeMap = segmentRangeMap;
        return this;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public ClickHousePartitionClean setDateFormat(final String dateFormat) {
        this.dateFormat = dateFormat;
        return this;
    }

    @Override
    protected void internalInit() {
        KylinConfig config = getConfig();
        val segments = getTargetSegments();
        val dataflowManager = NDataflowManager.getInstance(config, getProject());
        val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, getProject());
        val tableFlowManager = SecondStorageUtil.tableFlowManager(config, getProject());
        Preconditions.checkState(nodeGroupManager.isPresent() && tableFlowManager.isPresent());
        val dataflow = dataflowManager.getDataflow(getParam(NBatchConstants.P_DATAFLOW_ID));
        val tableFlow = Objects.requireNonNull(tableFlowManager.get().get(dataflow.getId()).orElse(null));
        setNodeCount(Math.toIntExact(nodeGroupManager.map(
                manager -> manager.listAll().stream().mapToLong(nodeGroup -> nodeGroup.getNodeNames().size()).sum())
                .orElse(0L)));
        segments.forEach(segment -> {
            nodeGroupManager.get().listAll().stream().flatMap(nodeGroup -> nodeGroup.getNodeNames().stream())
                    .forEach(node -> {
                        database = NameUtil.getDatabase(dataflow);
                        for (TableData tableData : tableFlow.getTableDataList()) {
                            table = NameUtil.getTable(dataflow, tableData.getLayoutID());

                            ShardCleaner shardCleaner = segmentRangeMap.get(segment).isInfinite()
                                    ? new ShardCleaner(node, database, table, null, true, null)
                                    : new ShardCleaner(node, database, table,
                                    SecondStorageDateUtils.splitByDay(segmentRangeMap.get(segment)), dateFormat);

                            shardCleaners.add(shardCleaner);
                        }
                    });
        });
    }

    @Override
    protected Runnable getTask(ShardCleaner shardCleaner) {
        return () -> {
            try {
                shardCleaner.cleanPartitions();
            } catch (SQLException e) {
                log.error("node {} clean partitions {} in {}.{} failed", shardCleaner.getClickHouse().getShardName(),
                        shardCleaner.getPartitions(), shardCleaner.getDatabase(), shardCleaner.getTable());
                ExceptionUtils.rethrow(e);
            }
        };
    }
}

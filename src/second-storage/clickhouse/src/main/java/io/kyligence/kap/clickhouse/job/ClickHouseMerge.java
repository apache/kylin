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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePartition;
import lombok.val;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseMerge extends ClickHouseLoad {
    private Set<String> oldSegmentIds;
    private String targetSegmentId;

    public ClickHouseMerge() {
        this.setName(SecondStorageConstants.STEP_MERGE_SECOND_STORAGE);
    }

    public ClickHouseMerge(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected void init() {
        super.init();
        oldSegmentIds = Sets.newHashSet(getParam(NBatchConstants.P_SEGMENT_IDS).split(","));
        targetSegmentId = getParam(SecondStorageConstants.P_MERGED_SEGMENT_ID);
        Preconditions.checkNotNull(targetSegmentId);
    }

    private TableFlow getDataFlow() {
        val tableFlowManager = SecondStorage.tableFlowManager(getConfig(), getProject());
        return tableFlowManager.get(getTargetSubject()).orElse(null);
    }

    @Override
    public Set<String> getSegmentIds() {
        if (oldSegmentIds == null || oldSegmentIds.isEmpty()) {
            return Collections.emptySet();
        }
        val tableFlow = getDataFlow();
        Preconditions.checkNotNull(tableFlow);
        val existedSegments = tableFlow.getTableDataList().stream()
                .flatMap(tableData -> tableData.getPartitions().stream()).map(TablePartition::getSegmentId)
                .collect(Collectors.toSet());
        return existedSegments.containsAll(oldSegmentIds) ? Collections.emptySet() : oldSegmentIds;
    }

    @Override
    protected void updateMeta() {
        if (!getSegmentIds().isEmpty()) {
            super.updateMeta();
        }
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            return getDataFlow().update(copied -> copied.getTableDataList().forEach(tableData -> {
                tableData.mergePartitions(oldSegmentIds, targetSegmentId);
            }));
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }
}

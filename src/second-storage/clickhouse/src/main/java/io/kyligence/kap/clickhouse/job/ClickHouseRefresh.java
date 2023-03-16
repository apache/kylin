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

import static io.kyligence.kap.secondstorage.SecondStorageConstants.P_OLD_SEGMENT_IDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.kylin.common.SegmentOnlineMode;
import org.apache.kylin.job.SecondStorageJobParamUtil;
import org.apache.kylin.job.handler.SecondStorageIndexCleanJobHandler;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageConstants;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.TableData;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import lombok.val;

@NotThreadSafe
public class ClickHouseRefresh extends ClickHouseLoad {
    private Map<String, String> newSegToOld = null;
    private Set<String> oldSegmentIds;

    public ClickHouseRefresh() {
        this.setName(SecondStorageConstants.STEP_REFRESH_SECOND_STORAGE);
    }

    public ClickHouseRefresh(Object notSetId) {
        super(notSetId);
    }

    private void initSegMap() {
        newSegToOld = new HashMap<>();

        String[] segmentIds = getParam(NBatchConstants.P_SEGMENT_IDS).split(",");
        String[] oldSegmentIdsArray = getParam(P_OLD_SEGMENT_IDS).split(",");

        oldSegmentIds = new HashSet<>(Arrays.asList(oldSegmentIdsArray));

        for (int i = 0; i < segmentIds.length; i++) {
            newSegToOld.put(segmentIds[i], oldSegmentIdsArray[i]);
        }
    }

    @Override
    protected List<LoadInfo> preprocessLoadInfo(List<LoadInfo> infoList) {
        if (newSegToOld == null) {
            initSegMap();
        }
        infoList.forEach(info -> info.setOldSegmentId(newSegToOld.get(info.getSegmentId())));
        return infoList;
    }

    @Override
    protected void beforeDataCommit() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            getTableFlow().update(copied -> copied.all().forEach(tableData ->
                    tableData.removePartitions(p -> oldSegmentIds.contains(p.getSegmentId()))
            ));
            return null;
        }, project, 1, getEpochId());
    }

    @Override
    protected void updateMeta() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            super.updateMeta();

            getTableFlow().update(copied -> copied.all().forEach(tableData ->
                    tableData.removePartitions(p -> oldSegmentIds.contains(p.getSegmentId()))
            ));

            Set<Long> needDeleteLayoutIds = getTableFlow().getTableDataList().stream()
                    .filter(tableData -> tableData.getPartitions().isEmpty())
                    .map(TableData::getLayoutID).collect(Collectors.toSet());

            getTableFlow().update(copied -> copied.cleanTableData(tableData -> tableData.getPartitions().isEmpty()));
            getTablePlan().update(t -> t.cleanTable(needDeleteLayoutIds));

            if (!needDeleteLayoutIds.isEmpty()) {
                val jobHandler = new SecondStorageIndexCleanJobHandler();
                final JobParam param = SecondStorageJobParamUtil.layoutCleanParam(project, getTargetSubject(), getSubmitter(), needDeleteLayoutIds, Collections.emptySet());
                JobManager.getInstance(getConfig(), project).addJob(param, jobHandler);
            }

            return null;
        }, project, 1, getEpochId());
    }

    private TableFlow getTableFlow() {
        val tableFlowManager = SecondStorage.tableFlowManager(getConfig(), getProject());
        return tableFlowManager.get(getTargetSubject()).orElse(null);
    }

    private TablePlan getTablePlan() {
        val tablePlanManager = SecondStorageUtil.tablePlanManager(getConfig(), project);
        Preconditions.checkState(tablePlanManager.isPresent());
        return tablePlanManager.get().get(getTargetSubject()).orElse(null);
    }

    @Override
    protected void updateDFSSegmentIfNeeded(MethodContext mc) {
        if (!isDAGJobScheduler()) {
            return;
        }

        if (!SegmentOnlineMode.ANY.toString().equalsIgnoreCase(getProjectConfig().getKylinEngineSegmentOnlineMode())) {
            return;
        }

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            final NDataflowManager dfMgr = NDataflowManager.getInstance(getConfig(), getProject());
            val dataFlow = dfMgr.getDataflow(mc.getDataflowId()).copy();
            NDataflowUpdate update = new NDataflowUpdate(mc.getDf().getId());
            List<NDataSegment> toUpdateSegments = new ArrayList<>(oldSegmentIds.size());
            List<NDataSegment> toRemoveSegments = new ArrayList<>(oldSegmentIds.size());

            for (Map.Entry<String, String> entry : newSegToOld.entrySet()) {
                String newSegmentId = entry.getKey();
                String oldSegmentId = entry.getValue();

                NDataSegment newSegment = dataFlow.getSegment(newSegmentId);
                NDataSegment oldSegment = dataFlow.getSegment(oldSegmentId);

                if (newSegment == null || newSegment.getStatus() != SegmentStatusEnum.NEW) {
                    continue;
                }

                newSegment.setStatus(SegmentStatusEnum.READY);
                toUpdateSegments.add(newSegment);

                if (oldSegment != null) {
                    toRemoveSegments.add(oldSegment);
                }
            }

            update.setToRemoveSegs(toRemoveSegments.toArray(new NDataSegment[0]));
            update.setToUpdateSegs(toUpdateSegments.toArray(new NDataSegment[0]));
            dfMgr.updateDataflowWithoutIndex(update);

            markDFStatus(mc.getDataflowId());
            return null;
        }, project, 1, getEpochId());
    }
}

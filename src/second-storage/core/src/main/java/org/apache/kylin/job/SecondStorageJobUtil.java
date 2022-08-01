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
package org.apache.kylin.job;

import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.common.ExecutableUtil;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.glassfish.jersey.internal.guava.Sets;
import org.msgpack.core.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CREATE_JOB;


public class SecondStorageJobUtil extends ExecutableUtil {

    private static boolean isValidateSegments(NDataSegment segment, SegmentRange<?> range) {
        return segment.getSegRange().startStartMatch(range)
                && segment.getSegRange().endEndMatch(range)
                && (segment.getStatus() == SegmentStatusEnum.READY
                ||
                segment.getStatus() == SegmentStatusEnum.WARNING
        );
    }

    @Override
    public void computeLayout(JobParam jobParam) {
        final KylinConfig config = KylinConfig.getInstanceFromEnv();
        final String project = jobParam.getProject();
        final String model = jobParam.getModel();
        final NDataflow df = NDataflowManager.getInstance(config, project)
                .getDataflow(model);

        List<NDataSegment> segments = df.getSegments().stream()
                .filter(segment -> isValidateSegments(segment, segment.getSegRange()))
                .collect(Collectors.toList());

        if (segments.isEmpty()) {
            throw new KylinException(FAILED_CREATE_JOB, MsgPicker.getMsg().getAddJobCheckSegmentReadyFail());
        }

        Manager<TablePlan> tablePlanManager = SecondStorage.tablePlanManager(config, project);
        Manager<TableFlow> tableFlowManager = SecondStorage.tableFlowManager(config, project);
        TablePlan plan = tablePlanManager.makeSureRootEntity(model);
        tableFlowManager.makeSureRootEntity(model);
        Map<Long, List<LayoutEntity>> layouts = segments.stream()
                .flatMap(nDataSegment -> nDataSegment.getLayoutsMap().values().stream())
                .map(NDataLayout::getLayout)
                .filter(SecondStorageUtil::isBaseTableIndex)
                .collect(Collectors.groupingBy(LayoutEntity::getIndexId));

        NDataModel dataModel = df.getModel();
        if (!segments.get(0).getSegRange().isInfinite()) {
            LayoutEntity baseTableIndex = segments.get(0).getIndexPlan().getBaseTableLayout();
            String partitionCol = dataModel.getPartitionDesc().getPartitionDateColumn();
            Preconditions.checkState(baseTableIndex.getColumns().stream()
                            .map(TblColRef::getTableDotName).anyMatch(col -> Objects.equals(col, partitionCol)),
                    "Table index should contains partition column " + partitionCol
            );
        }

        Set<LayoutEntity> processed = Sets.newHashSet();
        for (Map.Entry<Long, List<LayoutEntity>> entry : layouts.entrySet()) {
            LayoutEntity layoutEntity =
                    entry.getValue().stream().filter(l -> l.isBaseIndex() && IndexEntity.isTableIndex(l.getId())).findFirst().orElse(null);
            if (layoutEntity != null) {
                processed.add(layoutEntity);
                plan = plan.createTableEntityIfNotExists(layoutEntity, true);
            }
        }
        jobParam.setProcessLayouts(new HashSet<>(processed));
    }
}

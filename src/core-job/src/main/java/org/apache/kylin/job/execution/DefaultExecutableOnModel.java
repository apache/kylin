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

package org.apache.kylin.job.execution;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.SegmentPartition;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

public class DefaultExecutableOnModel extends DefaultExecutable {

    @Getter
    @Setter
    private ExecutableHandler handler;

    public DefaultExecutableOnModel() {
        super();
    }

    public DefaultExecutableOnModel(Object notSetId) {
        super(notSetId);
    }

    private String getTargetModel() {
        return getTargetSubject();
    }

    @Override
    public boolean checkSuicide() {
        try {
            return !checkAnyTargetSegmentAndPartitionExists() || !checkAnyLayoutExists();
        } catch (Exception e) {
            return true;
        }
    }

    public boolean checkAnyLayoutExists() {
        String layouts = getParam(NBatchConstants.P_LAYOUT_IDS);
        if (StringUtils.isEmpty(layouts)) {
            return true;
        }
        val indexPlanManager = NIndexPlanManager.getInstance(getConfig(), getProject());
        val indexPlan = indexPlanManager.getIndexPlan(getTargetModel());
        val allLayoutIds = indexPlan.getAllLayouts().stream().map(l -> l.getId() + "").collect(Collectors.toSet());
        return Stream.of(StringHelper.splitAndTrim(layouts, ",")).anyMatch(allLayoutIds::contains);
    }

    private boolean checkTargetSegmentAndPartitionExists(String segmentId) {
        NDataflow dataflow = NDataflowManager.getInstance(getConfig(), getProject()).getDataflow(getTargetModel());
        if (dataflow == null || dataflow.checkBrokenWithRelatedInfo()) {
            return false;
        }
        NDataSegment segment = dataflow.getSegment(segmentId);
        // segment is deleted or model multi partition
        if (segment == null) {
            return false;
        }
        if (dataflow.getModel().isMultiPartitionModel()) {
            Set<Long> partitionIds = segment.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                    .collect(Collectors.toSet());
            Set<Long> partitionInSegment = getPartitionsBySegment().get(segmentId);
            if (partitionInSegment == null) {
                logger.warn("Segment {} doesn't contain any partition in this job", segmentId);
                return true;
            }
            for (long partition : partitionInSegment) {
                if (!partitionIds.contains(partition)) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkAnyTargetSegmentAndPartitionExists() {
        List<String> topJobTargetSegments = getTargetSegments();
        AbstractExecutable parent = getParent();
        if (parent != null) {
            topJobTargetSegments = parent.getTargetSegments();
        }

        Preconditions.checkState(!topJobTargetSegments.isEmpty());
        return topJobTargetSegments.stream().anyMatch(this::checkTargetSegmentAndPartitionExists);
    }

    public boolean checkCuttingInJobByModel() {
        AbstractExecutable parent = getParent();
        if (parent == null) {
            parent = this;
        }
        if (!JobParam.isBuildIndexJob(parent.getJobType())) {
            return false;
        }
        val model = ((DefaultExecutableOnModel) parent).getTargetModel();
        return ExecutableManager.getInstance(getConfig(), getProject()).countCuttingInJobByModel(model, parent) > 0;
    }

    @Override
    public void onExecuteDiscardHook(String jobId) {
        if (handler != null) {
            handler.handleDiscardOrSuicidal();
        }
    }

    @Override
    protected void onExecuteSuicidalHook(String jobId) {
        if (handler != null) {
            handler.handleDiscardOrSuicidal();
        }
    }

}

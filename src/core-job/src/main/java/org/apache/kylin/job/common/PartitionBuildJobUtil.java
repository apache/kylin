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

package org.apache.kylin.job.common;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class PartitionBuildJobUtil extends ExecutableUtil {
    @Override
    public void computeLayout(JobParam jobParam) {
        // Partition build job only support in one segment.
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        HashSet<LayoutEntity> layouts = Sets.newHashSet();
        val segment = df.getSegment(jobParam.getSegment());
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getIndexPlan(jobParam.getModel());

        // segment is first built or other partition jobs (first built in this segment) are running
        if (segment.getMultiPartitions().isEmpty() || segment.getLayoutsMap().isEmpty()) {
            val execManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject());
            val executables = execManager.listMultiPartitionModelExec(jobParam.getModel(), ExecutableState::isRunning,
                    null, null, jobParam.getTargetSegments());
            if (executables.size() > 0) {
                Set<Long> layoutIds = executables.get(0).getLayoutIds();
                indexPlan.getAllLayouts().forEach(layout -> {
                    if (layoutIds.contains(layout.getId())) {
                        layouts.add(layout);
                    }
                });
            } else {
                layouts.addAll(indexPlan.getAllLayouts());
            }
        } else {
            segment.getLayoutsMap().values().forEach(layout -> layouts.add(layout.getLayout()));
        }
        jobParam.setProcessLayouts(filterTobeDelete(layouts));
        checkLayoutsNotEmpty(jobParam);
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        if (CollectionUtils.isEmpty(jobParam.getTargetPartitions())) {
            throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY);
        }
        // Partitions already in segments should not be built again.
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        NDataSegment segment = df.getSegment(jobParam.getSegment());
        segment.getMultiPartitions().forEach(partition -> {
            if (jobParam.getTargetPartitions().contains(partition.getPartitionId())
                    && partition.getStatus() == PartitionStatusEnum.READY) {
                throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_ABANDON);
            }
        });
    }
}

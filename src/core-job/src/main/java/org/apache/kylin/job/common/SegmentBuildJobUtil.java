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

import java.util.HashSet;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;

import com.google.common.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class SegmentBuildJobUtil extends ExecutableUtil {
    @Override
    public void computeLayout(JobParam jobParam) {
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getIndexPlan(jobParam.getModel());

        final HashSet<LayoutEntity> toBeProcessedLayouts = Sets.newLinkedHashSet();
        val targetLayouts = jobParam.getTargetLayouts();

        if (targetLayouts.isEmpty()) {
            toBeProcessedLayouts.addAll(indexPlan.getAllLayouts());
        } else {
            HashSet<Long> target = new HashSet(jobParam.getTargetLayouts());
            indexPlan.getAllLayouts().forEach(layout -> {
                if (target.contains(layout.getId())) {
                    toBeProcessedLayouts.add(layout);
                }
            });
        }

        jobParam.setProcessLayouts(filterTobeDelete(toBeProcessedLayouts));
        checkLayoutsNotEmpty(jobParam);
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        log.info("No need to comput partitions. Target partitions are {}", jobParam.getTargetPartitions());
    }
}

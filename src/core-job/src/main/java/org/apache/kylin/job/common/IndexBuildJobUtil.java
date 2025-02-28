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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_SEGMENT_READY_FAIL;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.Segments;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

/**
 *
 **/
@Slf4j
public class IndexBuildJobUtil extends ExecutableUtil {

    static {
        registerImplementation(JobTypeEnum.INDEX_BUILD, new IndexBuildJobUtil());
    }

    @Override
    public void computeLayout(JobParam jobParam) {
        NDataflow df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getDataflow(jobParam.getModel());
        IndexPlan indexPlan = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject())
                .getIndexPlan(jobParam.getModel());
        final HashSet<LayoutEntity> toBeProcessedLayouts = Sets.newLinkedHashSet();
        final HashSet<LayoutEntity> toBeDeletedLayouts = Sets.newLinkedHashSet();

        var readySegs = new Segments<>(df.getSegments(jobParam.getTargetSegments()));
        if (readySegs.isEmpty()) {
            log.warn("JobParam {} is no longer valid because no ready segment exists in target index_plan {}", jobParam,
                    jobParam.getModel());
            throw new KylinException(JOB_CREATE_CHECK_SEGMENT_READY_FAIL);
        }
        val mixLayouts = SegmentUtil.intersectionLayouts(readySegs);
        var allLayouts = indexPlan.getAllLayouts();
        val targetLayouts = jobParam.getTargetLayouts();

        if (targetLayouts.isEmpty()) {
            // No specified layouts and may have layouts deletion after build
            jobParam.setLayoutsDeletableAfterBuild(true);
            allLayouts.forEach(layout -> {
                if (!layout.isToBeDeleted() && !mixLayouts.contains(layout.getId())) {
                    toBeProcessedLayouts.add(layout);
                }
            });
            toBeDeletedLayouts.addAll(pruneTBDelLayouts(
                    allLayouts.stream().filter(LayoutEntity::isToBeDeleted)
                            .collect(Collectors.toCollection(LinkedHashSet::new)),
                    toBeProcessedLayouts, df, indexPlan, readySegs));
        } else {
            // Has user specified layouts to build and should not delete layouts after build for forward compatibility
            jobParam.setLayoutsDeletableAfterBuild(false);
            allLayouts.forEach(layout -> {
                long layoutId = layout.getId();
                if (targetLayouts.contains(layoutId) && !mixLayouts.contains(layoutId)) {
                    toBeProcessedLayouts.add(layout);
                }
            });
        }

        jobParam.setProcessLayouts(filterTobeDelete(toBeProcessedLayouts));
        jobParam.setDeleteLayouts(toBeDeletedLayouts);
        log.info("toBeProcessedLayouts: {}, toBeDeletedLayouts: {}",
                jobParam.getProcessLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList()),
                jobParam.getDeleteLayouts().stream().map(LayoutEntity::getId).collect(Collectors.toList()));
    }

    @Override
    public void computePartitions(JobParam jobParam) {
        NDataflowManager dfm = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), jobParam.getProject());
        val df = dfm.getDataflow(jobParam.getModel());
        val segments = df.getSegments(jobParam.getTargetSegments());
        val partitionIds = Sets.<Long> newHashSet();
        segments.forEach(segment -> {
            if (CollectionUtils.isEmpty(segment.getAllPartitionIds())) {
                throw new KylinException(JOB_CREATE_CHECK_MULTI_PARTITION_EMPTY);
            }
            partitionIds.addAll(segment.getAllPartitionIds());
        });
        jobParam.setTargetPartitions(partitionIds);
    }

    public static Set<LayoutEntity> pruneTBDelLayouts(Set<LayoutEntity> tbDelLayouts, Set<LayoutEntity> tbProLayouts,
            NDataflow df, IndexPlan indexPlan, List<NDataSegment> readySegs) {
        Set<LayoutEntity> newTBDelLayouts = Sets.newLinkedHashSet();
        if (tbDelLayouts.isEmpty()) {
            return newTBDelLayouts;
        }

        for (LayoutEntity layout : tbDelLayouts.stream().filter(LayoutEntity::isBaseIndex)
                .collect(Collectors.toList())) {
            if (baseLayoutDeletable(layout, tbProLayouts, df, readySegs)) {
                newTBDelLayouts.add(layout);
            }
        }

        List<LayoutEntity> allLayouts = indexPlan.getAllLayouts();
        boolean regularLayoutDeletable = true;
        Set<LayoutEntity> tbDelBaseLayouts = Collections.unmodifiableSet(newTBDelLayouts);
        for (LayoutEntity layout : tbDelLayouts.stream().filter(le -> !le.isBaseIndex()).collect(Collectors.toList())) {
            if (regularLayoutDeletable) {
                for (NDataSegment segment : df.getSegments()) {
                    if (segmentLayoutsNotFull(segment, tbDelBaseLayouts, tbProLayouts, allLayouts, readySegs)) {
                        regularLayoutDeletable = false;
                        break;
                    }
                }
                if (regularLayoutDeletable) {
                    newTBDelLayouts.add(layout);
                }
            }
        }
        return newTBDelLayouts;
    }

    private static boolean baseLayoutDeletable(LayoutEntity layout, Set<LayoutEntity> tbProLayouts, NDataflow df,
            List<NDataSegment> readySegs) {
        List<NDataSegment> segmentsHaveTheTBDel = df.getSegments().stream()
                .filter(segment -> segment.getLayoutIds().contains(layout.getId())).collect(Collectors.toList());
        LayoutEntity newBaseLayout = tbProLayouts.stream().filter(LayoutEntity::isBaseIndex)
                .filter(lay -> filterSameTypeLayout(lay, layout)).findFirst().orElse(null);
        if (newBaseLayout != null) {
            Set<NDataSegment> segmentsHaveTheTBPro = df.getSegments().stream()
                    .filter(segment -> segment.getLayoutIds().contains(newBaseLayout.getId()))
                    .collect(Collectors.toSet());
            segmentsHaveTheTBPro.addAll(readySegs);

            return segmentsHaveTheTBDel.stream().allMatch(segTBDLayout -> segmentsHaveTheTBPro.stream()
                    .anyMatch(segTBPLayout -> segTBPLayout.getId().equals(segTBDLayout.getId())));
        }
        return false;
    }

    private static boolean filterSameTypeLayout(LayoutEntity target, LayoutEntity refer) {
        return IndexEntity.isTableIndex(refer.getId()) ? IndexEntity.isTableIndex(target.getId())
                : IndexEntity.isAggIndex(target.getId());
    }

    private static boolean segmentLayoutsNotFull(NDataSegment segment, Set<LayoutEntity> tbDelBaseLayouts,
            Set<LayoutEntity> tbProLayouts, List<LayoutEntity> allLayouts, List<NDataSegment> readySegs) {

        if (segment.getLayoutIds().size() != allLayouts.size()) {
            Set<Long> segmentLayouts = Sets.newHashSet(segment.getLayoutIds());
            // For segment not belongs to readySegs
            segmentLayouts.removeAll(tbDelBaseLayouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet()));
            if (readySegs.stream().anyMatch(seg -> seg.getId().equals(segment.getId()))) {
                // For segment in readySegs
                segmentLayouts.addAll(tbProLayouts.stream().map(LayoutEntity::getId).collect(Collectors.toSet()));
            }
            return segmentLayouts.size() != allLayouts.size() - tbDelBaseLayouts.size();
        }
        return false;
    }
}

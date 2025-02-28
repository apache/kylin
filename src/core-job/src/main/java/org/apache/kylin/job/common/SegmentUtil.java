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

import static org.apache.kylin.job.execution.JobTypeEnum.INC_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.INDEX_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SUB_PARTITION_BUILD;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.PartitionStatusEnum;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.model.Segments;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentUtil {

    public static Segments<NDataSegment> getSegmentsExcludeRefreshingAndMerging(Segments<NDataSegment> segments) {
        Segments<NDataSegment> result = new Segments<>();
        Set<String> allIndexJobRunningSegments = segments.getFirstSegment() == null ? null
                : getAllIndexJobRunningSegments(segments.getFirstSegment().getModel());
        for (val seg : segments) {
            val status = getSegmentStatusToDisplay(segments, seg, null, allIndexJobRunningSegments);
            if (!(Objects.equals(SegmentStatusEnumToDisplay.REFRESHING, status)
                    || Objects.equals(SegmentStatusEnumToDisplay.MERGING, status))) {
                result.add(seg);
            }
        }
        return result;
    }

    // Shouldn't fetch all the run segments first without thinking about it, for example this may affect
    // the performance of fetching the model list
    // TODO Consider decoupling with the SegmentUtil#getAllIndexJobRunningSegments
    public static <T extends ISegment> SegmentStatusEnumToDisplay getSegmentStatusToDisplay(Segments segments,
            T segment, List<AbstractExecutable> executables, Set<String> allIndexJobRunningSegments) {
        Segments<T> overlapSegs = segments.getSegmentsByRange(segment.getSegRange());
        overlapSegs.remove(segment);
        if (SegmentStatusEnum.NEW == segment.getStatus()) {
            if (!CollectionUtils.isEmpty(overlapSegs)
                    && overlapSegs.get(0).getSegRange().entireOverlaps(segment.getSegRange())) {
                return SegmentStatusEnumToDisplay.REFRESHING;
            }

            if (CollectionUtils.isEmpty(overlapSegs) || anyIncSegmentJobRunning(segment)) {
                return SegmentStatusEnumToDisplay.LOADING;
            }

            return SegmentStatusEnumToDisplay.MERGING;
        }

        if (isAnyPartitionLoading(segment)) {
            return SegmentStatusEnumToDisplay.LOADING;
        }

        if (isAnyPartitionRefreshing(segment)) {
            return SegmentStatusEnumToDisplay.REFRESHING;
        }

        if (CollectionUtils.isNotEmpty(overlapSegs)) {
            if (CollectionUtils.isEmpty(overlapSegs.getSegments(SegmentStatusEnum.NEW))) {
                return SegmentStatusEnumToDisplay.OVERLAP;
            }
            return SegmentStatusEnumToDisplay.LOCKED;
        }

        if (anyIndexJobRunning(segment, executables, allIndexJobRunningSegments)) {
            return SegmentStatusEnumToDisplay.LOADING;
        }

        if (SegmentStatusEnum.WARNING == segment.getStatus()) {
            return SegmentStatusEnumToDisplay.WARNING;
        }

        return SegmentStatusEnumToDisplay.ONLINE;
    }

    protected static <T extends ISegment> boolean anyIndexJobRunning(T segment) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ExecutableManager execManager = ExecutableManager.getInstance(kylinConfig, segment.getModel().getProject());
        List<AbstractExecutable> executables = execManager.listExecByModelAndStatus(segment.getModel().getId(),
                ExecutableState::isRunning, INDEX_BUILD, SUB_PARTITION_BUILD);
        return executables.stream().anyMatch(task -> task.getSegmentIds().contains(segment.getId()));
    }

    protected static <T extends ISegment> boolean anyIncSegmentJobRunning(T segment) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ExecutableManager execManager = ExecutableManager.getInstance(kylinConfig, segment.getModel().getProject());
        List<AbstractExecutable> executables = execManager.listExecByModelAndStatus(segment.getModel().getId(),
                ExecutableState::isRunning, INC_BUILD);
        return executables.stream().anyMatch(task -> task.getSegmentIds().contains(segment.getId()));
    }

    protected static <T extends ISegment> boolean anyIndexJobRunning(T segment, List<AbstractExecutable> executables,
            Set<String> allIndexJobRunningSegments) {
        if (Objects.isNull(executables)) {
            return allIndexJobRunningSegments == null ? anyIndexJobRunning(segment)
                    : allIndexJobRunningSegments.contains(segment.getId());
        } else {
            return executables.stream().anyMatch(task -> task.getSegmentIds().contains(segment.getId()));
        }
    }

    // Calling this method takes into account the case of an empty segment
    public static Set<String> getAllIndexJobRunningSegments(NDataModel model) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ExecutableManager execManager = ExecutableManager.getInstance(kylinConfig, model.getProject());
        List<AbstractExecutable> executables = execManager.listExecByModelAndStatus(model.getId(),
                ExecutableState::isRunning, INDEX_BUILD, SUB_PARTITION_BUILD);
        return executables.stream().flatMap(task -> task.getSegmentIds().stream()).collect(Collectors.toSet());
    }

    private static <T extends ISegment> boolean isAnyPartitionLoading(T segment) {
        Preconditions.checkArgument(segment instanceof NDataSegment);
        val partitions = ((NDataSegment) segment).getMultiPartitions();

        if (CollectionUtils.isEmpty(partitions))
            return false;
        val loadingPartition = partitions.stream() //
                .filter(partition -> PartitionStatusEnum.NEW == partition.getStatus()) //
                .findAny().orElse(null);
        return loadingPartition != null;
    }

    private static <T extends ISegment> boolean isAnyPartitionRefreshing(T segment) {
        Preconditions.checkArgument(segment instanceof NDataSegment);
        val partitions = ((NDataSegment) segment).getMultiPartitions();

        if (CollectionUtils.isEmpty(partitions))
            return false;
        val refreshPartition = partitions.stream()
                .filter(partition -> PartitionStatusEnum.REFRESH == partition.getStatus()).findAny().orElse(null);
        return refreshPartition != null;
    }

    /**
     * Valid segment：
     * 1. SegmentStatusEnum is READY or WARNING.
     * 2. Time doesn't overlap with running segments.
     */
    public static Segments<NDataSegment> getValidSegments(String modelId, String project) {
        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelId);

        JobMapperFilter jobMapperFilter = new JobMapperFilter();
        jobMapperFilter.setProject(project);
        jobMapperFilter.setModelIds(Lists.newArrayList(modelId));
        jobMapperFilter.setStatuses(ExecutableState.getNotFinalStates());
        List<JobInfo> runningJobInfoList = JobContextUtil.getJobInfoDao(KylinConfig.getInstanceFromEnv())
                .getJobInfoListByFilter(jobMapperFilter);
        ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<AbstractExecutable> executables = runningJobInfoList.stream()
                .map(jobInfo -> executableManager.fromPO(JobInfoUtil.deserializeExecutablePO(jobInfo)))
                .collect(Collectors.toList());

        val runningSegs = new Segments<NDataSegment>();
        executables.stream().filter(e -> e.getTargetSegments() != null) //
                .flatMap(e -> e.getTargetSegments().stream()) //
                .distinct() //
                .filter(segId -> df.getSegment(segId) != null) //
                .forEach(segId -> runningSegs.add(df.getSegment(segId)));
        return getSegmentsByTime(df.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING), runningSegs);
    }

    public static Segments<NDataSegment> getSegmentsByTime(Segments<NDataSegment> targetSegments,
            Segments<NDataSegment> checkSegments) {
        val filterSegs = new Segments<NDataSegment>();
        for (NDataSegment targetSeg : targetSegments) {
            boolean isOverLap = false;
            for (NDataSegment relatedSeg : checkSegments) {
                if (targetSeg.getSegRange().overlaps(relatedSeg.getSegRange())) {
                    isOverLap = true;
                    break;
                }
            }
            if (!isOverLap) {
                filterSegs.add(targetSeg);
            }
        }
        return filterSegs;
    }

    public static Set<Long> intersectionLayouts(Segments<NDataSegment> segments) {
        HashSet<Long> layoutIds = Sets.newHashSet();
        if (segments.isEmpty()) {
            return layoutIds;
        }
        layoutIds = new HashSet<>(segments.get(0).getLayoutsMap().keySet());
        for (NDataSegment segment : segments) {
            if (segment.getLayoutsMap().size() == 0) {
                layoutIds.clear();
                break;
            }
            layoutIds.retainAll(segment.getLayoutsMap().keySet());
        }
        return layoutIds;
    }
}

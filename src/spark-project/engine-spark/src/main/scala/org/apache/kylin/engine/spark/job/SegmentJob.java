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

package org.apache.kylin.engine.spark.job;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.scheduler.JobRuntime;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.spark.tracker.BuildContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

public abstract class SegmentJob extends SparkApplication {

    private static final Logger logger = LoggerFactory.getLogger(SegmentJob.class);

    private static final String COMMA = ",";
    public JobRuntime runtime;
    protected IndexPlan indexPlan;
    protected String dataflowId;
    protected Set<LayoutEntity> readOnlyLayouts;
    protected Set<NDataSegment> readOnlySegments;
    // Resource detection results output path
    protected Path rdSharedPath;
    protected BuildContext buildContext;
    private boolean partialBuild = false;

    public boolean isPartialBuild() {
        return partialBuild;
    }

    public Set<LayoutEntity> getReadOnlyLayouts() {
        return readOnlyLayouts;
    }

    @Override
    protected final void extraInit() {
        partialBuild = Boolean.parseBoolean(getParam(NBatchConstants.P_PARTIAL_BUILD));
        Set<String> segmentIDs = Arrays.stream(getParam(NBatchConstants.P_SEGMENT_IDS).split(COMMA))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<Long> layoutIDs = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));
        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        rdSharedPath = config.getJobTmpShareDir(project, jobId);
        indexPlan = dataflowManager.getDataflow(dataflowId).getIndexPlan();

        readOnlyLayouts = Collections.unmodifiableSet(NSparkCubingUtil.toLayouts(indexPlan, layoutIDs));

        final Predicate<NDataSegment> notSkip = (NDataSegment dataSegment) -> !needSkipSegment(dataSegment);

        String excludeTableStr = getParam(NBatchConstants.P_EXCLUDED_TABLES);
        ImmutableSet<String> tables = StringUtils.isBlank(excludeTableStr) //
                ? ImmutableSet.of()
                : ImmutableSet.copyOf(excludeTableStr.split(SegmentJob.COMMA));
        readOnlySegments = Collections.unmodifiableSet((Set<? extends NDataSegment>) segmentIDs.stream() //
                .map(segmentId -> {
                    NDataSegment dataSegment = getSegment(segmentId);
                    dataSegment.setExcludedTables(tables);
                    return dataSegment;
                }).filter(notSkip) //
                .collect(Collectors.toCollection(LinkedHashSet::new)));
        runtime = new JobRuntime(config.getSegmentExecMaxThreads());
    }

    @Override
    public void extraDestroy() {
        super.extraDestroy();
        if (runtime != null) {
            runtime.shutdown();
        }
    }

    public String getDataflowId() {
        return dataflowId;
    }

    protected Path getRdSharedPath() {
        return rdSharedPath;
    }

    public Set<JobBucket> getReadOnlyBuckets() {
        return Collections.unmodifiableSet(ExecutableParams.getBuckets(getParam(NBatchConstants.P_BUCKETS)));
    }

    public NDataflow getDataflow(String dataflowId) {
        return getDataflowManager().getDataflow(dataflowId);
    }

    public NDataSegment getSegment(String segmentId) {
        // Always get the latest data segment.
        return getDataflowManager().getDataflow(dataflowId).getSegment(segmentId);
    }

    public final List<NDataSegment> getUnmergedSegments(NDataSegment merged) {
        List<NDataSegment> unmerged = getDataflowManager().getDataflow(dataflowId).getMergingSegments(merged);
        Preconditions.checkNotNull(unmerged);
        Preconditions.checkState(!unmerged.isEmpty());
        Collections.sort(unmerged);
        return unmerged;
    }

    public boolean needBuildSnapshots() {
        String s = getParam(NBatchConstants.P_NEED_BUILD_SNAPSHOTS);
        if (StringUtils.isBlank(s)) {
            return true;
        }
        return Boolean.parseBoolean(s);
    }

    protected boolean isPartitioned() {
        return Objects.nonNull(indexPlan.getModel().getMultiPartitionDesc());
    }

    private boolean needSkipSegment(NDataSegment dataSegment) {
        if (Objects.isNull(dataSegment)) {
            logger.info("Skip segment: NULL.");
            return true;
        }
        if (Objects.isNull(dataSegment.getSegRange()) || Objects.isNull(dataSegment.getModel())
                || Objects.isNull(dataSegment.getIndexPlan())) {
            logger.info("Skip segment: {}, range: {}, model: {}, index plan: {}", dataSegment.getId(),
                    dataSegment.getSegRange(), dataSegment.getModel(), dataSegment.getIndexPlan());
            return true;
        }
        return false;
    }

    private NDataflowManager getDataflowManager() {
        return NDataflowManager.getInstance(config, project);
    }

    public BuildContext getBuildContext() {
        return buildContext;
    }
}

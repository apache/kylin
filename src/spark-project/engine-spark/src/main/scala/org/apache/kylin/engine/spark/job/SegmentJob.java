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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.scheduler.JobRuntime;
import org.apache.kylin.job.execution.ExecutableParams;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.job.JobBucket;
import org.apache.spark.tracker.BuildContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import lombok.val;

public abstract class SegmentJob extends SparkApplication {

    private static final Logger logger = LoggerFactory.getLogger(SegmentJob.class);

    private static final String COMMA = ",";

    protected IndexPlan indexPlan;

    protected String dataflowId;
    // TODO change the `readOnlyLayouts`
    // In order to support the cube planner, `readOnlyLayouts` can be changed
    private Set<LayoutEntity> readOnlyLayouts;
    protected Set<NDataSegment> readOnlySegments;

    // Resource detection results output path
    protected Path rdSharedPath;

    protected JobRuntime runtime;

    private boolean partialBuild = false;

    protected BuildContext buildContext;

    public boolean isPartialBuild() {
        return partialBuild;
    }

    public Set<LayoutEntity> getReadOnlyLayouts() {
        return readOnlyLayouts;
    }

    private Set<LayoutEntity> recommendAggLayouts = new HashSet<>();

    public Set<LayoutEntity> getRecommendAggLayouts() {
        return recommendAggLayouts;
    }

    public void setRecommendAggLayouts(Set<LayoutEntity> aggIndexLayouts) {
        recommendAggLayouts.clear();
        recommendAggLayouts.addAll(aggIndexLayouts);
    }

    public void addMockIndex() {
        Set<LayoutEntity> set = new HashSet<>();
        List<Integer> colOrder = new ArrayList<>();
        colOrder.add(1);
        colOrder.addAll(indexPlan.getEffectiveMeasures().keySet());
        LayoutEntity aggLayout = indexPlan.createRecommendAggIndexLayout(colOrder);
        set.add(aggLayout);

        colOrder = new ArrayList<>();
        colOrder.add(1);
        colOrder.add(2);
        colOrder.addAll(indexPlan.getEffectiveMeasures().keySet());
        LayoutEntity aggLayout2 = indexPlan.createRecommendAggIndexLayout(colOrder);
        set.add(aggLayout2);

        setRecommendAggLayouts(set);
    }

    public boolean updateIndexPlanIfNeed() {
        // when run the cube planner, there will be some recommended index layouts for this model
        if (getRecommendAggLayouts().size() != 0) {
            UnitOfWork.doInTransactionWithRetry(() -> {
                // update and add the recommended index layout to the index plan
                val recommendAggLayouts = Lists.newArrayList(getRecommendAggLayouts());
                NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
                logger.info("Update the index plan and add recommended agg index {}", recommendAggLayouts);
                indexPlanManager.updateIndexPlan(dataflowId, copyForWrite -> {
                    copyForWrite.createAndAddRecommendAggIndex(recommendAggLayouts);
                });
                return null;
            }, project);
            updateJobLayoutsIfNeed();
            return true;
        } else {
            logger.info("There is no recommended agg index");
            return false;
        }
    }

    private void updateJobLayoutsIfNeed() {
        // update this job layouts when there are recommended index layouts for this build
        NDataflowManager dataflowManager = NDataflowManager.getInstance(config, project);
        // get the new index plan
        indexPlan = dataflowManager.getDataflow(dataflowId).getIndexPlan();
        // get the new layout
        val newJobLayouts = indexPlan.getAllLayouts();
        logger.info("Update Job layouts from {} to {}", readOnlyLayouts, newJobLayouts);
        readOnlyLayouts = new HashSet<>(newJobLayouts);
        // rewrite the `P_LAYOUT_IDS` parameters
        setParam(NBatchConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(readOnlyLayouts)));
    }

    @Override
    protected void extraInit() {
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
        if (Objects.nonNull(runtime)) {
            runtime.shutdown();
        }
        if (Objects.nonNull(buildContext)) {
            buildContext.stop();
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

    public JobRuntime getRuntime() {
        return runtime;
    }

    public BuildContext getBuildContext() {
        return buildContext;
    }
}

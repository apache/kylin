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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.scheduler.JobRuntime;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;

public abstract class SegmentJob extends SparkApplication {

    private static final Logger logger = LoggerFactory.getLogger(SegmentJob.class);

    private static final String COMMA = ",";

    protected IndexPlan indexPlan;

    protected String dataflowId;
    // In order to support the cost based planner, `readOnlyLayouts` can be changed
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

    private Set<List<Integer>> recommendAggColOrders = new HashSet<>();

    public void setRecommendAggColOrders(Set<List<Integer>> recommendAggColOrders) {
        this.recommendAggColOrders = recommendAggColOrders;
    }

    public Set<List<Integer>> getRecommendAggColOrders() {
        return recommendAggColOrders;
    }

    public boolean updateIndexPlanIfNeed() {
        // when run the cost based index planner, there will be some recommended index layouts for this model
        if (getRecommendAggColOrders().size() != 0) {
            UnitOfWork.doInTransactionWithRetry(() -> {
                // update and add the recommended index layout to the index plan
                val recommendAggLayouts = Lists.newArrayList(getRecommendAggColOrders());
                NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
                logger.debug("Update the index plan and add recommended agg index {}", recommendAggLayouts);
                indexPlanManager.updateIndexPlan(dataflowId, copyForWrite -> {
                    // construct the map: colOrder of layout -> id
                    val allRuleLayouts = copyForWrite.getRuleBasedIndex().genCuboidLayouts();
                    Map<List<Integer>, Long> colOrder2Id = Maps.newHashMap();
                    allRuleLayouts.forEach(layoutEntity -> {
                        colOrder2Id.put(layoutEntity.getColOrder(), layoutEntity.getId());
                    });
                    logger.debug("All rule base layouts {}", allRuleLayouts);
                    Set<Long> costBasedResult = Sets.newHashSet();
                    for (List<Integer> colOrder : recommendAggLayouts) {
                        if (colOrder2Id.containsKey(colOrder)) {
                            costBasedResult.add(colOrder2Id.get(colOrder));
                        } else {
                            logger.debug("Can't find the layout {} in the rule base index", colOrder);
                        }
                    }
                    // reset the rule base layouts
                    logger.debug("Set the rule pruning cost based list layouts {}", costBasedResult);
                    val ruleBaseIndex = copyForWrite.getRuleBasedIndex();
                    ruleBaseIndex.setLayoutsOfCostBasedList(costBasedResult);
                    copyForWrite.setRuleBasedIndex(ruleBaseIndex);
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
        logger.debug("Update Job layouts count from {} to {}", readOnlyLayouts.size(), newJobLayouts.size());
        readOnlyLayouts = new HashSet<>(newJobLayouts);
        // rewrite the `P_LAYOUT_IDS` parameters
        setParam(NBatchConstants.P_LAYOUT_IDS, NSparkCubingUtil.ids2Str(NSparkCubingUtil.toLayoutIds(readOnlyLayouts)));
    }

    @Override
    protected void extraInit() {
        super.extraInit();
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

        readOnlySegments = Collections.unmodifiableSet((Set<? extends NDataSegment>) segmentIDs.stream() //
                .map(this::getSegment).filter(notSkip) //
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
        return Objects.nonNull(indexPlan.getModel().getPartitionDesc())
                && Objects.nonNull(indexPlan.getModel().getMultiPartitionDesc());
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

    public IndexPlan getIndexPlan() {
        return indexPlan;
    }
}

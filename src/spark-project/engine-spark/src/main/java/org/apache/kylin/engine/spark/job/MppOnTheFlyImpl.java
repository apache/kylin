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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.cache.kylin.KylinCacheFileSystem;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.job.step.ParamPropagation;
import org.apache.kylin.engine.spark.job.step.build.MaterializeFactView;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableSet;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.query.plugin.runtime.MppOnTheFlyProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MppOnTheFlyImpl implements MppOnTheFlyProvider {

    /**
     * Compute a layout on-the-fly for segments that don't have it.
     * <p/>
     * Return null if all segments have tne required layout.
     */
    @Override
    public LogicalPlan computeMissingLayout(List<NDataSegment> prunedSegments, long layoutId, SparkSession ss) {
        List<NDataSegment> missingLayoutSegs = prunedSegments.stream()
                .filter(seg -> !seg.getLayoutsMap().containsKey(layoutId)).collect(Collectors.toList());

        // quick return if all segments have the layout
        if (missingLayoutSegs.isEmpty())
            return null;

        // make a virtual segment, with a merged SegmentRange
        NDataSegment virtualSeg = virtualSegment(missingLayoutSegs);

        // enable use of local cache of remote source files
        KylinCacheFileSystem.setAcceptCacheTimeLocally(virtualSeg.getDataflow().getLastModified());

        // compute
        return computeLayout(virtualSeg, layoutId, ss);
    }

    private NDataSegment virtualSegment(List<NDataSegment> missingLayoutSegs) {
        Preconditions.checkState(missingLayoutSegs.size() > 0);

        SegmentRange<?> merged = null;
        for (NDataSegment seg : missingLayoutSegs) {
            if (merged == null)
                merged = seg.getSegRange();
            else
                merged = merged.coverWith(seg.getSegRange());
        }

        NDataSegment ret = new NDataSegment(missingLayoutSegs.get(0).getDataflow(), merged);
        ret.setId(""); // to skip DFBuilderHelper.checkPointSegment()
        return ret;
    }

    /**
     * Mimic the logic of building cuboid layout from SegmentBuildJob.build()
     * <p/>
     * Work hard to reuse the existing SegmentJob & FlatTableStage etc.
     */
    public LogicalPlan computeLayout(NDataSegment virtualSeg, long layoutId, SparkSession ss) {
        KylinConfig config = virtualSeg.getDataflow().getConfig();
        KylinBuildEnv.getOrCreate(config);
        try {

            LayoutEntity layoutEntity = virtualSeg.getIndexPlan().getLayoutEntity(layoutId);
            MockJobContext jobContext = new MockJobContext(virtualSeg, layoutId, ss);
            ParamPropagation params = new ParamPropagation();

            MaterializeFactView tool = new MaterializeFactView(jobContext, virtualSeg, params);
            Dataset<Row> layoutDS = tool.computeLayoutFromSourceAllInOne(layoutEntity);

            return layoutDS == null ? null : layoutDS.queryExecution().logical();

        } finally {
            KylinBuildEnv.clean();
        }
    }

    // just work as a JobContext
    public static class MockJobContext extends SegmentJob {

        private final NDataSegment seg;

        private MockJobContext(NDataSegment seg, long layoutId, SparkSession ss) {
            this.seg = seg;
            this.ss = ss;

            this.dataflowId = seg.getDataflow().getId();
            this.indexPlan = seg.getIndexPlan();
            this.readOnlySegments = ImmutableSet.of(seg);
            this.readOnlyLayouts = ImmutableSet.of(seg.getIndexPlan().getLayoutEntity(layoutId));
        }

        @Override
        public KylinConfig getConfig() {
            return seg.getDataflow().getConfig();
        }

        @Override
        public SparkSession getSparkSession() {
            return ss;
        }

        @Override
        public boolean isPartialBuild() {
            return false;
        }

        // empty
        @Override
        public IJobProgressReport getReport() {
            return new IJobProgressReport() {
                @Override
                public boolean updateSparkJobInfo(Map<String, String> params, String url, String json) {
                    return false;
                }

                @Override
                public boolean updateSparkJobExtraInfo(Map<String, String> params, String url, String project,
                        String jobId, Map<String, String> extraInfo) {
                    return false;
                }
            };
        }

        // empty
        @Override
        protected void doExecute() throws Exception {
            // nothing to do
        }
    }
}

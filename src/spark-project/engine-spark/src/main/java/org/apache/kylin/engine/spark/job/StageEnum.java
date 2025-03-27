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

import java.util.Objects;

import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.job.step.ParamPropagation;
import org.apache.kylin.engine.spark.job.step.ResourceWaitStage;
import org.apache.kylin.engine.spark.job.step.StageExec;
import org.apache.kylin.engine.spark.job.step.build.BuildDict;
import org.apache.kylin.engine.spark.job.step.build.BuildFlatTableStats;
import org.apache.kylin.engine.spark.job.step.build.BuildLayer;
import org.apache.kylin.engine.spark.job.step.build.CostBasedPlanner;
import org.apache.kylin.engine.spark.job.step.build.MaterializeFactView;
import org.apache.kylin.engine.spark.job.step.build.MaterializeFlatTable;
import org.apache.kylin.engine.spark.job.step.build.RefreshColumnBytes;
import org.apache.kylin.engine.spark.job.step.build.RefreshSnapshot;
import org.apache.kylin.engine.spark.job.step.build.partition.PartitionBuildDict;
import org.apache.kylin.engine.spark.job.step.build.partition.PartitionBuildLayer;
import org.apache.kylin.engine.spark.job.step.build.partition.PartitionCostBasedPlanner;
import org.apache.kylin.engine.spark.job.step.build.partition.PartitionGatherFlatTableStats;
import org.apache.kylin.engine.spark.job.step.build.partition.PartitionMaterializeFactView;
import org.apache.kylin.engine.spark.job.step.build.partition.PartitionMaterializeFlatTable;
import org.apache.kylin.engine.spark.job.step.build.partition.PartitionRefreshColumnBytes;
import org.apache.kylin.engine.spark.job.step.merge.MergeColumnBytes;
import org.apache.kylin.engine.spark.job.step.merge.MergeFlatTable;
import org.apache.kylin.engine.spark.job.step.merge.MergeIndices;
import org.apache.kylin.engine.spark.job.step.merge.partition.PartitionMergeColumnBytes;
import org.apache.kylin.engine.spark.job.step.merge.partition.PartitionMergeFlatTable;
import org.apache.kylin.engine.spark.job.step.merge.partition.PartitionMergeIndices;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.StageExecutable;
import org.apache.kylin.metadata.cube.model.NDataSegment;

public enum StageEnum {

    WAIT_FOR_RESOURCE {

        @Override
        public StageExec createExec(SparkApplication app) {
            return new ResourceWaitStage(Objects.requireNonNull(app));
        }

        @Override
        public StageExec createExec(SegmentJob job) {
            return new ResourceWaitStage(Objects.requireNonNull(job));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_WAIT_FOR_RESOURCE);
        }
    },

    REFRESH_SNAPSHOT {

        @Override
        public StageExec createExec(SegmentJob job) {
            return new RefreshSnapshot(Objects.requireNonNull(job));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_REFRESH_SNAPSHOT);
        }
    },

    MATERIALIZE_FACT_VIEW {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment, ParamPropagation params) {
            return Objects.requireNonNull(job).isPartitioned()
                    ? new PartitionMaterializeFactView(job, Objects.requireNonNull(segment),
                            Objects.requireNonNull(params))
                    : new MaterializeFactView(job, Objects.requireNonNull(segment), Objects.requireNonNull(params));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_MATERIALIZED_FACT_TABLE);
        }
    },

    BUILD_GLOBAL_DICT {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment, ParamPropagation params) {
            return Objects.requireNonNull(job).isPartitioned()
                    ? new PartitionBuildDict(job, Objects.requireNonNull(segment), Objects.requireNonNull(params))
                    : new BuildDict(job, Objects.requireNonNull(segment), Objects.requireNonNull(params));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_BUILD_GLOBAL_DICT);
        }
    },

    MATERIALIZE_FLAT_TABLE {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment, ParamPropagation params) {
            return Objects.requireNonNull(job).isPartitioned()
                    ? new PartitionMaterializeFlatTable(job, Objects.requireNonNull(segment),
                            Objects.requireNonNull(params))
                    : new MaterializeFlatTable(job, Objects.requireNonNull(segment), Objects.requireNonNull(params));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_GENERATE_FLAT_TABLE);
        }
    },

    COST_BASED_PLANNER {
        @Override
        public StageExec createExec(SegmentJob jobContext, NDataSegment dataSegment, ParamPropagation buildParam) {
            if (jobContext.isPartitioned()) {
                return new PartitionCostBasedPlanner(jobContext, dataSegment, buildParam);
            }
            return new CostBasedPlanner(jobContext, dataSegment, buildParam);
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_COST_BASED_PLANNER);
        }
    },

    BUILD_FLAT_TABLE_STATS {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment, ParamPropagation params) {
            return Objects.requireNonNull(job).isPartitioned()
                    ? new PartitionGatherFlatTableStats(job, Objects.requireNonNull(segment),
                            Objects.requireNonNull(params))
                    : new BuildFlatTableStats(job, Objects.requireNonNull(segment), Objects.requireNonNull(params));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_GATHER_FLAT_TABLE_STATS);
        }
    },

    BUILD_LAYER {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment, ParamPropagation params) {
            return Objects.requireNonNull(job).isPartitioned()
                    ? new PartitionBuildLayer(job, Objects.requireNonNull(segment), Objects.requireNonNull(params))
                    : new BuildLayer(job, Objects.requireNonNull(segment), Objects.requireNonNull(params));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_BUILD_LAYER);
        }
    },

    REFRESH_COLUMN_BYTES {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment, ParamPropagation params) {
            return Objects.requireNonNull(job).isPartitioned() //
                    ? new PartitionRefreshColumnBytes(job, Objects.requireNonNull(segment),
                            Objects.requireNonNull(params)) //
                    : new RefreshColumnBytes(job, Objects.requireNonNull(segment), Objects.requireNonNull(params));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_REFRESH_COLUMN_BYTES);
        }
    },

    MERGE_FLAT_TABLE {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment) {
            return Objects.requireNonNull(job).isPartitioned() //
                    ? new PartitionMergeFlatTable(job, Objects.requireNonNull(segment)) //
                    : new MergeFlatTable(job, Objects.requireNonNull(segment));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_MERGE_FLAT_TABLE);
        }
    },

    MERGE_INDICES {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment) {
            return Objects.requireNonNull(job).isPartitioned() //
                    ? new PartitionMergeIndices(job, Objects.requireNonNull(segment)) //
                    : new MergeIndices(job, Objects.requireNonNull(segment));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_MERGE_INDICES);
        }
    },

    MERGE_COLUMN_BYTES {

        @Override
        public StageExec createExec(SegmentJob job, NDataSegment segment) {
            return Objects.requireNonNull(job).isPartitioned() //
                    ? new PartitionMergeColumnBytes(job, Objects.requireNonNull(segment))
                    : new MergeColumnBytes(job, Objects.requireNonNull(segment));
        }

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_MERGE_COLUMN_BYTES);
        }
    },

    TABLE_SAMPLING {

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_TABLE_SAMPLING);
        }
    },

    BUILD_SNAPSHOT {

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_BUILD_SNAPSHOT);
        }
    },

    DELETE_USELESS_LAYOUT_DATA {

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_DELETE_USELESS_LAYOUT_DATA);
        }
    },

    OPTIMIZE_LAYOUT_DATA_BY_REPARTITION {

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_OPTIMIZE_LAYOUT_DATA_REPARTITION);
        }
    },

    OPTIMIZE_LAYOUT_DATA_BY_ZORDER {

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_OPTIMIZE_LAYOUT_DATA_ZORDER);
        }
    },

    OPTIMIZE_LAYOUT_DATA_BY_COMPACTION {

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_OPTIMIZE_LAYOUT_DATA_COMPACTION);
        }
    },

    INTERNAL_TABLE_LOAD {

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_INTERNAL_TABLE_LOAD);
        }
    },

    OPTIMIZE_INDEX_PLAN {

        @Override
        public StageExecutable createExecutable() {
            return new StageExecutable(ExecutableConstants.STAGE_NAME_OPTIMIZE_INDEX_PLAN);
        }
    };

    public StageExec createExec(SparkApplication app) {
        throw new UnsupportedOperationException();
    }

    public StageExec createExec(SegmentJob job) {
        throw new UnsupportedOperationException();
    }

    public StageExec createExec(SegmentJob job, NDataSegment segment) {
        throw new UnsupportedOperationException();
    }

    public StageExec createExec(SegmentJob job, NDataSegment segment, ParamPropagation params) {
        throw new UnsupportedOperationException();
    }

    public void createThenExecute(SegmentJob job) {
        createExec(job).doExecute();
    }

    public abstract StageExecutable createExecutable();

}

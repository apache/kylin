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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.StageBase;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.job.exec.BuildExec;
import org.apache.kylin.engine.spark.job.stage.BuildParam;
import org.apache.kylin.engine.spark.job.stage.StageExec;
import org.apache.kylin.engine.spark.job.stage.WaiteForResource;
import org.apache.kylin.engine.spark.job.stage.build.BuildDict;
import org.apache.kylin.engine.spark.job.stage.build.BuildLayer;
import org.apache.kylin.engine.spark.job.stage.build.GatherFlatTableStats;
import org.apache.kylin.engine.spark.job.stage.build.GenerateFlatTable;
import org.apache.kylin.engine.spark.job.stage.build.MaterializedFactTableView;
import org.apache.kylin.engine.spark.job.stage.build.RefreshColumnBytes;
import org.apache.kylin.engine.spark.job.stage.build.RefreshSnapshots;
import org.apache.kylin.engine.spark.job.stage.build.partition.PartitionBuildDict;
import org.apache.kylin.engine.spark.job.stage.build.partition.PartitionBuildLayer;
import org.apache.kylin.engine.spark.job.stage.build.partition.PartitionGatherFlatTableStats;
import org.apache.kylin.engine.spark.job.stage.build.partition.PartitionGenerateFlatTable;
import org.apache.kylin.engine.spark.job.stage.build.partition.PartitionMaterializedFactTableView;
import org.apache.kylin.engine.spark.job.stage.build.partition.PartitionRefreshColumnBytes;
import org.apache.kylin.engine.spark.job.stage.merge.MergeColumnBytes;
import org.apache.kylin.engine.spark.job.stage.merge.MergeFlatTable;
import org.apache.kylin.engine.spark.job.stage.merge.MergeIndices;
import org.apache.kylin.engine.spark.job.stage.merge.partition.PartitionMergeColumnBytes;
import org.apache.kylin.engine.spark.job.stage.merge.partition.PartitionMergeFlatTable;
import org.apache.kylin.engine.spark.job.stage.merge.partition.PartitionMergeIndices;
import org.apache.kylin.engine.spark.job.stage.snapshots.SnapshotsBuild;
import org.apache.kylin.engine.spark.job.stage.tablesampling.AnalyzerTable;
import org.apache.kylin.engine.spark.job.step.NStageForBuild;
import org.apache.kylin.engine.spark.job.step.NStageForMerge;
import org.apache.kylin.engine.spark.job.step.NStageForSnapshot;
import org.apache.kylin.engine.spark.job.step.NStageForTableSampling;
import org.apache.kylin.engine.spark.job.step.NStageForWaitingForYarnResource;
import org.apache.kylin.engine.spark.stats.analyzer.TableAnalyzerJob;
import org.apache.kylin.metadata.cube.model.NDataSegment;

public enum StageType {
    WAITE_FOR_RESOURCE {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            return new WaiteForResource(jobContext);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForWaitingForYarnResource(ExecutableConstants.STAGE_NAME_WAITE_FOR_RESOURCE);
        }
    },
    REFRESH_SNAPSHOTS {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            return new RefreshSnapshots((SegmentJob) jobContext);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_REFRESH_SNAPSHOTS);
        }
    },
    MATERIALIZED_FACT_TABLE {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionMaterializedFactTableView((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new MaterializedFactTableView((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_MATERIALIZED_FACT_TABLE);
        }
    },
    BUILD_DICT {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionBuildDict((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new BuildDict((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_BUILD_DICT);
        }
    },
    GENERATE_FLAT_TABLE {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionGenerateFlatTable((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new GenerateFlatTable((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_GENERATE_FLAT_TABLE);
        }
    },
    GATHER_FLAT_TABLE_STATS {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionGatherFlatTableStats((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new GatherFlatTableStats((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_GATHER_FLAT_TABLE_STATS);
        }
    },
    BUILD_LAYER {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionBuildLayer((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new BuildLayer((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_BUILD_LAYER);
        }
    },
    REFRESH_COLUMN_BYTES {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionRefreshColumnBytes((SegmentJob) jobContext, dataSegment, buildParam);
            }
            return new RefreshColumnBytes((SegmentJob) jobContext, dataSegment, buildParam);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForBuild(ExecutableConstants.STAGE_NAME_REFRESH_COLUMN_BYTES);
        }
    },

    MERGE_FLAT_TABLE {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionMergeFlatTable((SegmentJob) jobContext, dataSegment);
            }
            return new MergeFlatTable((SegmentJob) jobContext, dataSegment);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForMerge(ExecutableConstants.STAGE_NAME_MERGE_FLAT_TABLE);
        }
    },
    MERGE_INDICES {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionMergeIndices((SegmentJob) jobContext, dataSegment);
            }
            return new MergeIndices((SegmentJob) jobContext, dataSegment);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForMerge(ExecutableConstants.STAGE_NAME_MERGE_INDICES);
        }
    },
    MERGE_COLUMN_BYTES {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            if (isPartitioned(jobContext)) {
                return new PartitionMergeColumnBytes((SegmentJob) jobContext, dataSegment);
            }
            return new MergeColumnBytes((SegmentJob) jobContext, dataSegment);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForMerge(ExecutableConstants.STAGE_NAME_MERGE_COLUMN_BYTES);
        }
    },
    TABLE_SAMPLING {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            return new AnalyzerTable((TableAnalyzerJob) jobContext);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForTableSampling(ExecutableConstants.STAGE_NAME_TABLE_SAMPLING);
        }
    },
    SNAPSHOT_BUILD {
        @Override
        public StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam) {
            return new SnapshotsBuild((SnapshotBuildJob) jobContext);
        }

        @Override
        protected StageBase create(NSparkExecutable parent, KylinConfig config) {
            return new NStageForSnapshot(ExecutableConstants.STAGE_NAME_SNAPSHOT_BUILD);
        }
    };

    protected boolean isPartitioned(SparkApplication jobContext) {
        return ((SegmentJob) jobContext).isPartitioned();
    }

    public abstract StageExec create(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam);

    protected abstract StageBase create(NSparkExecutable parent, KylinConfig config);

    public StageExec createStage(SparkApplication jobContext, NDataSegment dataSegment, BuildParam buildParam,
            BuildExec exec) {
        final StageExec step = create(jobContext, dataSegment, buildParam);
        exec.addStage(step);
        return step;
    }

    public StageBase createStage(NSparkExecutable parent, KylinConfig config) {
        final StageBase step = create(parent, config);
        parent.addStage(step);
        return step;
    }
}

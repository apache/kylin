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

import static org.apache.kylin.engine.spark.job.StageEnum.BUILD_FLAT_TABLE_STATS;
import static org.apache.kylin.engine.spark.job.StageEnum.BUILD_GLOBAL_DICT;
import static org.apache.kylin.engine.spark.job.StageEnum.BUILD_LAYER;
import static org.apache.kylin.engine.spark.job.StageEnum.COST_BASED_PLANNER;
import static org.apache.kylin.engine.spark.job.StageEnum.DELETE_USELESS_LAYOUT_DATA;
import static org.apache.kylin.engine.spark.job.StageEnum.INTERNAL_TABLE_LOAD;
import static org.apache.kylin.engine.spark.job.StageEnum.MATERIALIZE_FACT_VIEW;
import static org.apache.kylin.engine.spark.job.StageEnum.MATERIALIZE_FLAT_TABLE;
import static org.apache.kylin.engine.spark.job.StageEnum.MERGE_COLUMN_BYTES;
import static org.apache.kylin.engine.spark.job.StageEnum.MERGE_FLAT_TABLE;
import static org.apache.kylin.engine.spark.job.StageEnum.MERGE_INDICES;
import static org.apache.kylin.engine.spark.job.StageEnum.OPTIMIZE_LAYOUT_DATA_BY_COMPACTION;
import static org.apache.kylin.engine.spark.job.StageEnum.OPTIMIZE_LAYOUT_DATA_BY_REPARTITION;
import static org.apache.kylin.engine.spark.job.StageEnum.OPTIMIZE_LAYOUT_DATA_BY_ZORDER;
import static org.apache.kylin.engine.spark.job.StageEnum.REFRESH_COLUMN_BYTES;
import static org.apache.kylin.engine.spark.job.StageEnum.REFRESH_SNAPSHOT;
import static org.apache.kylin.engine.spark.job.StageEnum.TABLE_SAMPLING;
import static org.apache.kylin.engine.spark.job.StageEnum.WAIT_FOR_RESOURCE;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.DefaultExecutableOnTable;
import org.apache.kylin.metadata.cube.model.NBatchConstants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum StepEnum {

    RESOURCE_DETECT {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            if (config.getSparkEngineBuildStepsToSkip().contains(NResourceDetectStep.class.getName())) {
                return null;
            }
            return new NResourceDetectStep(parent);
        }
    },

    CLEANUP_AFTER_MERGE {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new NSparkCleanupAfterMergeStep();
        }
    },

    CUBING {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new NSparkCubingStep(config.getSparkBuildClassName());
        }

        @Override
        protected void addStage(NSparkExecutable step) {
            step.addStage(WAIT_FOR_RESOURCE);
            step.addStage(REFRESH_SNAPSHOT);
            step.addStage(MATERIALIZE_FACT_VIEW);
            step.addStage(BUILD_GLOBAL_DICT);
            step.addStage(MATERIALIZE_FLAT_TABLE);

            String enablePlanner = StringUtils.trim(step.getParam(NBatchConstants.P_JOB_ENABLE_PLANNER));
            if (StringUtils.equalsIgnoreCase(enablePlanner, KylinConfigBase.TRUE)) {
                step.addStage(COST_BASED_PLANNER);
            }

            step.addStage(BUILD_FLAT_TABLE_STATS);
            step.addStage(BUILD_LAYER);
            step.addStage(REFRESH_COLUMN_BYTES);
        }
    },

    MERGING {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new NSparkMergingStep(config.getSparkMergeClassName());
        }

        @Override
        protected void addStage(NSparkExecutable step) {
            step.addStage(WAIT_FOR_RESOURCE);

            step.addStage(MERGE_FLAT_TABLE);
            step.addStage(MERGE_INDICES);
            step.addStage(MERGE_COLUMN_BYTES);
        }
    },

    BUILD_SNAPSHOT {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new NSparkSnapshotBuildingStep(config.getSnapshotBuildClassName());
        }

        @Override
        protected void addStage(NSparkExecutable step) {
            step.addStage(WAIT_FOR_RESOURCE);
            step.addStage(StageEnum.BUILD_SNAPSHOT);
        }
    },

    BUILD_INTERNAL {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new InternalTableLoadingStep(config.getInternalTableBuildClassName());
        }

        @Override
        protected void addStage(NSparkExecutable step) {
            step.addStage(WAIT_FOR_RESOURCE);
            step.addStage(INTERNAL_TABLE_LOAD);
        }
    },

    SAMPLING {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new NTableSamplingJob.SamplingStep(config.getSparkTableSamplingClassName());
        }

        @Override
        protected void addStage(NSparkExecutable step) {
            step.addStage(WAIT_FOR_RESOURCE);
            step.addStage(TABLE_SAMPLING);
        }
    },

    UPDATE_METADATA {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            if (!(parent instanceof DefaultExecutableOnModel)) {
                throw new IllegalArgumentException();
            }
            ((DefaultExecutableOnModel) parent)
                    .setHandler(ExecutableHandlerFactory.createExecutableHandler((DefaultExecutableOnModel) parent));
            return new NSparkUpdateMetadataStep();
        }
    },

    CLEANUP_TRANSACTIONAL_TABLE {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new SparkCleanupTransactionalTableStep();
        }
    },

    LOAD_GLUTEN_CACHE {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            if (parent instanceof DefaultExecutableOnTable) {
                return new InternalTableLoadCacheStep();
            }
            return new ModelIndexLoadCacheStep();
        }
    },

    LAYOUT_DATA_OPTIMIZE {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new NSparkLayoutDataOptimizeStep(config.getSparkOptimizeClassName());
        }

        @Override
        protected void addStage(NSparkExecutable step) {
            step.addStage(WAIT_FOR_RESOURCE);
            step.addStage(DELETE_USELESS_LAYOUT_DATA);
            step.addStage(OPTIMIZE_LAYOUT_DATA_BY_REPARTITION);
            step.addStage(OPTIMIZE_LAYOUT_DATA_BY_ZORDER);
            step.addStage(OPTIMIZE_LAYOUT_DATA_BY_COMPACTION);
        }
    },

    OPTIMIZE_INDEX_PLAN {
        @Override
        protected AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config) {
            return new NSparkIndexPlanOptimizeJob.IndexPlanOptimizeStep(config.getSparkIndexPlanOptClassName());
        }

        @Override
        protected void addStage(NSparkExecutable step) {
            step.addStage(WAIT_FOR_RESOURCE);
            step.addStage(StageEnum.OPTIMIZE_INDEX_PLAN);
        }
    };

    protected abstract AbstractExecutable createInner(DefaultExecutable parent, KylinConfig config);

    /**
     * add step in spark executable
     */
    protected void addStage(NSparkExecutable step) {
        // do nothing.
    }

    public final AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
        AbstractExecutable step = createInner(parent, config);
        if (step == null) {
            log.info("{} skipped", this);
        } else {
            addParam(parent, step);
        }
        return step;
    }

    protected void addParam(DefaultExecutable parent, AbstractExecutable step) {
        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        step.setJobType(parent.getJobType());
        parent.addTask(step);
        if (step instanceof NSparkExecutable) {
            addStage((NSparkExecutable) step);
            ((NSparkExecutable) step).setStageMap();

            ((NSparkExecutable) step).setDistMetaUrl(
                    KylinConfig.readSystemKylinConfig().getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        }
        if (CollectionUtils.isNotEmpty(parent.getTargetPartitions())) {
            step.setTargetPartitions(parent.getTargetPartitions());
        }
    }

}

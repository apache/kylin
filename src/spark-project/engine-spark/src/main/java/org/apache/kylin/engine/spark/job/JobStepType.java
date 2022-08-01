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

import static org.apache.kylin.engine.spark.job.StageType.BUILD_DICT;
import static org.apache.kylin.engine.spark.job.StageType.BUILD_LAYER;
import static org.apache.kylin.engine.spark.job.StageType.GATHER_FLAT_TABLE_STATS;
import static org.apache.kylin.engine.spark.job.StageType.GENERATE_FLAT_TABLE;
import static org.apache.kylin.engine.spark.job.StageType.MATERIALIZED_FACT_TABLE;
import static org.apache.kylin.engine.spark.job.StageType.MERGE_COLUMN_BYTES;
import static org.apache.kylin.engine.spark.job.StageType.MERGE_FLAT_TABLE;
import static org.apache.kylin.engine.spark.job.StageType.MERGE_INDICES;
import static org.apache.kylin.engine.spark.job.StageType.REFRESH_COLUMN_BYTES;
import static org.apache.kylin.engine.spark.job.StageType.REFRESH_SNAPSHOTS;
import static org.apache.kylin.engine.spark.job.StageType.SNAPSHOT_BUILD;
import static org.apache.kylin.engine.spark.job.StageType.TABLE_SAMPLING;
import static org.apache.kylin.engine.spark.job.StageType.WAITE_FOR_RESOURCE;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.SecondStorageStepFactory;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum JobStepType {

    RESOURCE_DETECT {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            if (config.getSparkEngineBuildStepsToSkip().contains(NResourceDetectStep.class.getName())) {
                return null;
            }
            return new NResourceDetectStep(parent);
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    CLEAN_UP_AFTER_MERGE {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            AbstractExecutable step = new NSparkCleanupAfterMergeStep();
            return step;

        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },
    CUBING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkCubingStep(config.getSparkBuildClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            REFRESH_SNAPSHOTS.createStage(parent, config);

            MATERIALIZED_FACT_TABLE.createStage(parent, config);
            BUILD_DICT.createStage(parent, config);
            GENERATE_FLAT_TABLE.createStage(parent, config);
            GATHER_FLAT_TABLE_STATS.createStage(parent, config);
            BUILD_LAYER.createStage(parent, config);
            REFRESH_COLUMN_BYTES.createStage(parent, config);
        }
    },
    MERGING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkMergingStep(config.getSparkMergeClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);

            MERGE_FLAT_TABLE.createStage(parent, config);
            MERGE_INDICES.createStage(parent, config);
            MERGE_COLUMN_BYTES.createStage(parent, config);
        }
    },

    BUILD_SNAPSHOT {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkSnapshotBuildingStep(config.getSnapshotBuildClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            SNAPSHOT_BUILD.createStage(parent, config);
        }
    },

    SAMPLING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NTableSamplingJob.SamplingStep(config.getSparkTableSamplingClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            TABLE_SAMPLING.createStage(parent, config);
        }
    },

    UPDATE_METADATA {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            if (!(parent instanceof DefaultChainedExecutableOnModel)) {
                throw new IllegalArgumentException();
            }
            ((DefaultChainedExecutableOnModel) parent).setHandler(
                    ExecutableHandlerFactory.createExecutableHandler((DefaultChainedExecutableOnModel) parent));
            return new NSparkUpdateMetadataStep();
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    SECOND_STORAGE_EXPORT {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return SecondStorageStepFactory.create(SecondStorageStepFactory.SecondStorageLoadStep.class, step -> {
                step.setProject(parent.getProject());
                step.setParams(parent.getParams());
            });
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    SECOND_STORAGE_REFRESH {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return SecondStorageStepFactory.create(SecondStorageStepFactory.SecondStorageRefreshStep.class, step -> {
                step.setProject(parent.getProject());
                step.setParams(parent.getParams());
            });
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    SECOND_STORAGE_MERGE {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return SecondStorageStepFactory.create(SecondStorageStepFactory.SecondStorageMergeStep.class, step -> {
                step.setProject(parent.getProject());
                step.setParams(parent.getParams());
            });
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    CLEAN_UP_TRANSACTIONAL_TABLE {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new SparkCleanupTransactionalTableStep();
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    };

    protected abstract AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config);

    /** add stage in spark executable */
    protected abstract void addSubStage(NSparkExecutable parent, KylinConfig config);

    public AbstractExecutable createStep(DefaultChainedExecutable parent, KylinConfig config) {
        AbstractExecutable step = create(parent, config);
        if (step == null) {
            log.info("{} skipped", this);
        } else {
            addParam(parent, step);
        }
        return step;
    }

    protected void addParam(DefaultChainedExecutable parent, AbstractExecutable step) {
        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        step.setJobType(parent.getJobType());
        parent.addTask(step);
        if (step instanceof NSparkExecutable) {
            addSubStage((NSparkExecutable) step, KylinConfig.readSystemKylinConfig());
            ((NSparkExecutable) step).setStageMap();

            ((NSparkExecutable) step).setDistMetaUrl(
                    KylinConfig.readSystemKylinConfig().getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        }
        if (CollectionUtils.isNotEmpty(parent.getTargetPartitions())) {
            step.setTargetPartitions(parent.getTargetPartitions());
        }
    }

}

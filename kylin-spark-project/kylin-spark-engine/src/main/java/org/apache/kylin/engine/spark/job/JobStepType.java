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
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

public enum JobStepType {

    RESOURCE_DETECT {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NResourceDetectStep(parent);
        }
    },

    SAMPLING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NTableSamplingStep(config.getSparkSampleTableClassName());
        }
    },

    CUBING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkCubingStep(config.getSparkBuildClassName());
        }
    },
    MERGING {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return new NSparkMergingStep(config.getSparkMergeClassName());
        }
    },

    CLEAN_UP_AFTER_MERGE {
        @Override
        public AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            AbstractExecutable step = new NSparkUpdateMetaAndCleanupAfterMergeStep();
            return step;

        }
    },
    MERGE_STATISTICS {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return null;
        }
    },
    OPTIMIZING {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return null;
        }
    },
    FILTER_RECOMMEND_CUBOID {
        @Override
        protected AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config) {
            return null;
        }
    };

    protected abstract AbstractExecutable create(DefaultChainedExecutable parent, KylinConfig config);

    public AbstractExecutable createStep(DefaultChainedExecutable parent, KylinConfig config) {
        AbstractExecutable step = create(parent, config);
        addParam(parent, step, config);
        return step;
    }

    protected void addParam(DefaultChainedExecutable parent, AbstractExecutable step, KylinConfig config) {
        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        step.setJobType(parent.getJobTypeEnum());
        parent.addTask(step);
        if (step instanceof NSparkExecutable) {
            ((NSparkExecutable) step).setDistMetaUrl(config.getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        }
    }
}

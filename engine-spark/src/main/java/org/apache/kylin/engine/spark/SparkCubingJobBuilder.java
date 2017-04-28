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
package org.apache.kylin.engine.spark;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class SparkCubingJobBuilder extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(SparkCubingJobBuilder.class);

    private final IMRInput.IMRBatchCubingInputSide inputSide;
    private final IMROutput2.IMRBatchCubingOutputSide2 outputSide;
    private final String confPath;
    private final String coprocessor;

    public SparkCubingJobBuilder(CubeSegment seg, String submitter, String confPath, String coprocessor) {
        super(seg, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        this.outputSide = MRUtil.getBatchCubingOutputSide2(seg);
        this.confPath = confPath;
        this.coprocessor = coprocessor;
    }

    public DefaultChainedExecutable build() {
        final CubingJob result = CubingJob.createBuildJob(seg, submitter, config);

        inputSide.addStepPhase1_CreateFlatTable(result);
        final IJoinedFlatTableDesc joinedFlatTableDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        final String tableName = joinedFlatTableDesc.getTableName();
        logger.info("intermediate table:" + tableName);

        final SparkExecutable sparkExecutable = new SparkExecutable();
        sparkExecutable.setClassName(SparkCubing.class.getName());
        sparkExecutable.setParam("hiveTable", tableName);
        sparkExecutable.setParam(CubingExecutableUtil.CUBE_NAME, seg.getRealization().getName());
        sparkExecutable.setParam("segmentId", seg.getUuid());
        sparkExecutable.setParam("confPath", confPath);
        sparkExecutable.setParam("coprocessor", coprocessor);
        result.addTask(sparkExecutable);
        return result;
    }
}

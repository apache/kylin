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

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.secondstorage.SecondStorageUtil;

public class NSparkCleanupAfterMergeStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkCleanupAfterMergeStep.class);

    public NSparkCleanupAfterMergeStep() {
        this.setName(ExecutableConstants.STEP_NAME_CLEANUP);
    }

    public NSparkCleanupAfterMergeStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {

        String name = getParam(NBatchConstants.P_DATAFLOW_ID);
        String[] segmentIds = StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS), ",");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NDataflow dataflow = NDataflowManager.getInstance(config, getProject()).getDataflow(name);

        boolean timeMachineEnabled = KylinConfig.getInstanceFromEnv().getTimeMachineEnabled();

        for (String segmentId : segmentIds) {
            String path = dataflow.getSegmentHdfsPath(segmentId);
            if (!SecondStorageUtil.isModelEnable(dataflow.getProject(), dataflow.getModel().getUuid())) {
                if (!timeMachineEnabled) {
                    try {
                        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(path));
                        logger.info("The segment {} in dataflow {} has been successfully deleted, path : {}", //
                                segmentId, name, path);
                    } catch (IOException e) {
                        logger.warn("Can not delete segment {} in dataflow {}." + //
                                " Please try workaround thru garbage clean manually.", segmentId, name, e);
                    }
                }
            } else {
                logger.info("ClickHouse is enabled for the model, please delete segments {} in dataflow {} manually.", //
                        segmentIds, name);
            }
        }

        return ExecuteResult.createSucceed();
    }

}

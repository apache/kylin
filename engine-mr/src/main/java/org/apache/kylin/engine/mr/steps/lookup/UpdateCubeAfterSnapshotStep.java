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

package org.apache.kylin.engine.mr.steps.lookup;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Save lookup snapshot information to cube metadata
 */
public class UpdateCubeAfterSnapshotStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(UpdateCubeAfterSnapshotStep.class);

    public UpdateCubeAfterSnapshotStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig kylinConfig = context.getConfig();
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);

        CubeInstance cube = cubeManager.getCube(LookupExecutableUtil.getCubeName(this.getParams()));
        List<String> segmentIDs = LookupExecutableUtil.getSegments(this.getParams());
        String lookupTableName = LookupExecutableUtil.getLookupTableName(this.getParams());
        DefaultChainedExecutable job = (DefaultChainedExecutable) getManager().getJob(LookupExecutableUtil.getJobID(this.getParams()));

        String contextKey = BatchConstants.LOOKUP_EXT_SNAPSHOT_CONTEXT_PFX + lookupTableName;
        String snapshotResPath = job.getExtraInfo(contextKey);

        CubeDesc cubeDesc = cube.getDescriptor();
        try {
            logger.info("update snapshot path to cube metadata");
            if (cubeDesc.isGlobalSnapshotTable(lookupTableName)) {
                LookupExecutableUtil.updateSnapshotPathToCube(cubeManager, cube, lookupTableName,
                        snapshotResPath);
            } else {
                LookupExecutableUtil.updateSnapshotPathToSegments(cubeManager, cube, segmentIDs, lookupTableName,
                        snapshotResPath);
            }
            return new ExecuteResult();
        } catch (IOException e) {
            logger.error("fail to save cuboid statistics", e);
            return ExecuteResult.createError(e);
        }
    }

}

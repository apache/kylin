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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class CopyDictionaryStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(CopyDictionaryStep.class);

    public CopyDictionaryStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams())).latestCopyForWrite();
        final CubeSegment optimizeSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        CubeSegment oldSegment = optimizeSegment.getCubeInstance().getOriginalSegmentToOptimize(optimizeSegment);
        Preconditions.checkNotNull(oldSegment,
                "cannot find the original segment to be optimized by " + optimizeSegment);

        // --- Copy dictionary
        optimizeSegment.getDictionaries().putAll(oldSegment.getDictionaries());
        optimizeSegment.getSnapshots().putAll(oldSegment.getSnapshots());
        optimizeSegment.getRowkeyStats().addAll(oldSegment.getRowkeyStats());

        try {
            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            cubeBuilder.setToUpdateSegs(optimizeSegment);
            mgr.updateCube(cubeBuilder);
        } catch (IOException e) {
            logger.error("fail to merge dictionary or lookup snapshots", e);
            return ExecuteResult.createError(e);
        }

        return new ExecuteResult();
    }
}

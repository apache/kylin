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

import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import java.io.IOException;
import java.util.List;

abstract class UpdateCubeInfoStep extends AbstractExecutable {

    private final CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
    private CubeInstance cube;

    UpdateCubeInfoStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        String cubeName = cube.getName();

        DistributedLock lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();
        if (!lock.lock(getLockPath(cubeName), Long.MAX_VALUE)) {
            throw new RuntimeException("Cannot acquire lock to cube" + cubeName);
        }

        try {
            cube = cubeManager.getCubeFromStore(cubeName);
        } catch (IOException e) {
            logger.error("fail to get CubeInstance from store: " + cubeName, e);
            throw new RuntimeException("fail to get CubeInstance from store", e);
        }

        ExecuteResult result;
        try {
            result = work(context, cube);
            if (result.succeed()) {
                keepCubeRetention(cubeName);
                mergeCubeSegment(cubeName);
            }
        } finally {
            lock.unlock(getLockPath(cubeName));
        }
        return result;
    }

    protected abstract ExecuteResult work(ExecutableContext context, CubeInstance cube) throws ExecuteException;

    private void keepCubeRetention(String cubeName) {
        CubeDesc desc = cube.getDescriptor();
        if (desc.getRetentionRange() <= 0) {
            return;
        }

        logger.info("checking keepCubeRetention");
        //get cube again to ensure the cube metadata is latest
        cube = cubeManager.getCube(cubeName);

        List<CubeSegment> readySegs = cube.getSegments(SegmentStatusEnum.READY);
        List<CubeSegment> toRemoveSegs = Lists.newArrayList();
        long tail = readySegs.get(readySegs.size() - 1).getDateRangeEnd();
        long head = tail - desc.getRetentionRange();
        for (CubeSegment seg : readySegs) {
            if (seg.getDateRangeEnd() > 0) { // for streaming cube its initial value is 0
                if (seg.getDateRangeEnd() <= head) {
                    toRemoveSegs.add(seg);
                }
            }
        }

        if (toRemoveSegs.size() > 0) {
            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            cubeBuilder.setToRemoveSegs(toRemoveSegs.toArray(new CubeSegment[toRemoveSegs.size()]));
            try {
                cubeManager.updateCube(cubeBuilder);
            } catch (IOException e) {
                logger.error("Failed to remove old segment from cube " + cubeName, e);
            }
        }
    }

    private void mergeCubeSegment(String cubeName) {
        if (!cube.needAutoMerge()) {
            return;
        }

        try {
            logger.info("auto merge segments");

            //get cube again to ensure the cube metadata is latest
            cube = cubeManager.getCube(cubeName);

            Pair<Long, Long> offsets = cubeManager.autoMergeCubeSegments(cube);
            if (offsets != null) {
                CubeSegment newSeg = cubeManager.mergeSegments(cube, 0, 0, offsets.getFirst(), offsets.getSecond(), true);
                logger.debug("Will submit merge job on " + newSeg);
                DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(newSeg, "SYSTEM");
                ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv()).addJob(job);
            } else {
                logger.debug("Not ready for merge on cube " + cubeName);
            }
        } catch (IOException e) {
            logger.error("Failed to auto merge cube " + cubeName, e);
        }
    }

    private String getLockPath(String pathName) {
        return "/metadata/" + pathName + "/lock";
    }
}

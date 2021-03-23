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

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.CuboidStatsReaderUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.UpdateCubeInfoAfterCheckpointStep;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class NSparkUpdateCubeInfoAfterOptimizeStep extends NSparkExecutable {
    private static final Logger logger = LoggerFactory.getLogger(UpdateCubeInfoAfterCheckpointStep.class);

    public NSparkUpdateCubeInfoAfterOptimizeStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()));

        Set<Long> recommendCuboids = cube.getCuboidsRecommend();
        try {
            List<CubeSegment> newSegments = cube.getSegments(SegmentStatusEnum.READY_PENDING);
            Map<Long, Long> recommendCuboidsWithStats = CuboidStatsReaderUtil
                    .readCuboidStatsFromSegments(recommendCuboids, newSegments);
            if (recommendCuboidsWithStats == null) {
                throw new RuntimeException("Fail to get statistics info for recommended cuboids after optimization!!!");
            }
            cubeManager.promoteCheckpointOptimizeSegments(cube, recommendCuboidsWithStats,
                    newSegments.toArray(new CubeSegment[newSegments.size()]));
            return new ExecuteResult();
        } catch (Exception e) {
            logger.error("fail to update cube after build", e);
            return ExecuteResult.createError(e);
        }
    }
}

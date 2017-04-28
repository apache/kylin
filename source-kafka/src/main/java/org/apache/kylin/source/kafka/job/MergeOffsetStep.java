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
package org.apache.kylin.source.kafka.job;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class MergeOffsetStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(MergeOffsetStep.class);

    public MergeOffsetStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final String segmentId = CubingExecutableUtil.getSegmentId(this.getParams());
        final CubeSegment segment = cube.getSegmentById(segmentId);

        Preconditions.checkNotNull(segment, "Cube segment '" + segmentId + "' not found.");
        List<CubeSegment> mergingSegs = cube.getMergingSegments(segment);

        Preconditions.checkArgument(mergingSegs.size() > 0, "Merging segment not exist.");

        Collections.sort(mergingSegs);
        final CubeSegment first = mergingSegs.get(0);
        final CubeSegment last = mergingSegs.get(mergingSegs.size() - 1);

        segment.setSourceOffsetStart(first.getSourceOffsetStart());
        segment.setSourcePartitionOffsetStart(first.getSourcePartitionOffsetStart());
        segment.setSourceOffsetEnd(last.getSourceOffsetEnd());
        segment.setSourcePartitionOffsetEnd(last.getSourcePartitionOffsetEnd());

        long dateRangeStart = CubeManager.minDateRangeStart(mergingSegs);
        long dateRangeEnd = CubeManager.maxDateRangeEnd(mergingSegs);

        segment.setDateRangeStart(dateRangeStart);
        segment.setDateRangeEnd(dateRangeEnd);

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToUpdateSegs(segment);
        try {
            cubeManager.updateCube(cubeBuilder);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to update cube segment offset", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

}

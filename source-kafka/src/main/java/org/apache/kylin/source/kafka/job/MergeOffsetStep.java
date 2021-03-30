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

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.Segments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

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
        final CubeInstance cubeCopy = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams())).latestCopyForWrite();
        final String segmentId = CubingExecutableUtil.getSegmentId(this.getParams());
        final CubeSegment segCopy = cubeCopy.getSegmentById(segmentId);

        Preconditions.checkNotNull(segCopy, "Cube segment '" + segmentId + "' not found.");
        Segments<CubeSegment> mergingSegs = cubeCopy.getMergingSegments(segCopy);

        Preconditions.checkArgument(mergingSegs.size() > 0, "Merging segment not exist.");

        Collections.sort(mergingSegs);
        final CubeSegment first = mergingSegs.get(0);
        final CubeSegment last = mergingSegs.get(mergingSegs.size() - 1);

        segCopy.setSegRange(new SegmentRange(first.getSegRange().start, last.getSegRange().end));
        segCopy.setSourcePartitionOffsetStart(first.getSourcePartitionOffsetStart());
        segCopy.setSourcePartitionOffsetEnd(last.getSourcePartitionOffsetEnd());

        segCopy.setTSRange(new TSRange(mergingSegs.getTSStart(), mergingSegs.getTSEnd()));

        CubeUpdate update = new CubeUpdate(cubeCopy);
        update.setToUpdateSegs(segCopy);
        try {
            cubeManager.updateCube(update);
            return ExecuteResult.createSucceed();
        } catch (IOException e) {
            logger.error("fail to update cube segment offset", e);
            return ExecuteResult.createError(e);
        }
    }

}

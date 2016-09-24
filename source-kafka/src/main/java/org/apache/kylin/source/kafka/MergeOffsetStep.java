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
package org.apache.kylin.source.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
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

import org.apache.kylin.source.kafka.util.KafkaOffsetMapping;

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
        final CubeSegment segment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        List<CubeSegment> mergingSegs = cube.getMergingSegments(segment);
        Map<Integer, Long> mergedStartOffsets = Maps.newHashMap();
        Map<Integer, Long> mergedEndOffsets = Maps.newHashMap();

        long dateRangeStart = Long.MAX_VALUE, dateRangeEnd = 0;
        for (CubeSegment seg: mergingSegs) {
            Map<Integer, Long> startOffsets = KafkaOffsetMapping.parseOffsetStart(seg);
            Map<Integer, Long> endOffsets = KafkaOffsetMapping.parseOffsetEnd(seg);

            for (Integer partition : startOffsets.keySet()) {
                long currentStart = mergedStartOffsets.get(partition) != null ? Long.valueOf(mergedStartOffsets.get(partition)) : Long.MAX_VALUE;
                long currentEnd = mergedEndOffsets.get(partition) != null ? Long.valueOf(mergedEndOffsets.get(partition)) : 0;
                mergedStartOffsets.put(partition, Math.min(currentStart, startOffsets.get(partition)));
                mergedEndOffsets.put(partition, Math.max(currentEnd, endOffsets.get(partition)));
            }
            dateRangeStart = Math.min(dateRangeStart, seg.getDateRangeStart());
            dateRangeEnd = Math.max(dateRangeEnd, seg.getDateRangeEnd());
        }

        KafkaOffsetMapping.saveOffsetStart(segment, mergedStartOffsets);
        KafkaOffsetMapping.saveOffsetEnd(segment, mergedEndOffsets);
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

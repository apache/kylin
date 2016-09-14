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

import org.apache.kylin.source.kafka.util.KafkaClient;
import org.apache.kylin.source.kafka.util.KafkaOffsetMapping;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class SeekOffsetStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(SeekOffsetStep.class);

    public SeekOffsetStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment segment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));

        Map<Integer, Long> startOffsets = KafkaOffsetMapping.parseOffsetStart(segment);
        Map<Integer, Long> endOffsets = KafkaOffsetMapping.parseOffsetEnd(segment);

        if (startOffsets.size() > 0 && endOffsets.size() > 0 && startOffsets.size() == endOffsets.size()) {
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "skipped, as the offset is provided.");
        }

        final KafkaConfig kafakaConfig = KafkaConfigManager.getInstance(context.getConfig()).getKafkaConfig(cube.getFactTable());
        final String brokers = KafkaClient.getKafkaBrokers(kafakaConfig);
        final String topic = kafakaConfig.getTopic();
        try (final KafkaConsumer consumer = KafkaClient.getKafkaConsumer(brokers, cube.getName(), null)) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

            if (startOffsets.isEmpty()) {
                // user didn't specify start offset, use the biggest offset in existing segments as start
                for (CubeSegment seg : cube.getSegments()) {
                    Map<Integer, Long> segEndOffset = KafkaOffsetMapping.parseOffsetEnd(seg);
                    for (PartitionInfo partition : partitionInfos) {
                        int partitionId = partition.partition();
                        if (segEndOffset.containsKey(partitionId)) {
                            startOffsets.put(partitionId, Math.max(startOffsets.containsKey(partitionId) ? startOffsets.get(partitionId) : 0, segEndOffset.get(partitionId)));
                        }
                    }
                }

                if (partitionInfos.size() > startOffsets.size()) {
                    // has new partition added
                    for (int x = startOffsets.size(); x < partitionInfos.size(); x++) {
                        long earliest = KafkaClient.getEarliestOffset(consumer, topic, partitionInfos.get(x).partition());
                        startOffsets.put(partitionInfos.get(x).partition(), earliest);
                    }
                }

                logger.info("Get start offset for segment " + segment.getName() + ": " + startOffsets.toString());
            }

            if (endOffsets.isEmpty()) {
                // user didn't specify end offset, use latest offset in kafka
                for (PartitionInfo partitionInfo : partitionInfos) {
                    long latest = KafkaClient.getLatestOffset(consumer, topic, partitionInfo.partition());
                    endOffsets.put(partitionInfo.partition(), latest);
                }

                logger.info("Get end offset for segment " + segment.getName() + ": " + endOffsets.toString());
            }
        }

        long totalStartOffset = 0, totalEndOffset = 0;
        for (Long v : startOffsets.values()) {
            totalStartOffset += v;
        }
        for (Long v : endOffsets.values()) {
            totalEndOffset += v;
        }

        if (totalEndOffset > totalStartOffset) {
            KafkaOffsetMapping.saveOffsetStart(segment, startOffsets);
            KafkaOffsetMapping.saveOffsetEnd(segment, endOffsets);
            segment.setName(CubeSegment.makeSegmentName(0, 0, totalStartOffset, totalEndOffset));
            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            cubeBuilder.setToUpdateSegs(segment);
            try {
                cubeManager.updateCube(cubeBuilder);
            } catch (IOException e) {
                return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
            }
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed, offset start: " + totalStartOffset + ", offset end: " + totalEndOffset);
        } else {
            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            cubeBuilder.setToRemoveSegs(segment);
            try {
                cubeManager.updateCube(cubeBuilder);
            } catch (IOException e) {
                return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
            }

            return new ExecuteResult(ExecuteResult.State.DISCARDED, "No new message comes");
        }


    }

}

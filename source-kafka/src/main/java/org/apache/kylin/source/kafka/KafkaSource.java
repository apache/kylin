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

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.SourcePartition;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

//used by reflection
public class KafkaSource implements ISource {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMRInput.class) {
            return (I) new KafkaMRInput();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }

    @Override
    public ReadableTable createReadableTable(TableDesc tableDesc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getMRDependentResources(TableDesc table) {
        List<String> dependentResources = Lists.newArrayList();
        dependentResources.add(KafkaConfig.concatResourcePath(table.getIdentity()));
        dependentResources.add(StreamingConfig.concatResourcePath(table.getIdentity()));
        return dependentResources;
    }

    @Override
    public SourcePartition parsePartitionBeforeBuild(IBuildable buildable, SourcePartition srcPartition) {
        checkSourceOffsets(srcPartition);
        final SourcePartition result = SourcePartition.getCopyOf(srcPartition);
        final CubeInstance cube = (CubeInstance) buildable;
        if (result.getStartOffset() == 0) {
            final CubeSegment last = cube.getLastSegment();
            if (last != null) {
                logger.debug("Last segment exists, continue from last segment " + last.getName() + "'s end position: " + last.getSourcePartitionOffsetEnd());
                // from last seg's end position
                result.setSourcePartitionOffsetStart(last.getSourcePartitionOffsetEnd());
            } else if (cube.getDescriptor().getPartitionOffsetStart() != null && cube.getDescriptor().getPartitionOffsetStart().size() > 0) {
                logger.debug("Last segment doesn't exist, use the start offset that be initiated previously: " + cube.getDescriptor().getPartitionOffsetStart());
                result.setSourcePartitionOffsetStart(cube.getDescriptor().getPartitionOffsetStart());
            } else {
                // from the topic's earliest offset;
                logger.debug("Last segment doesn't exist, and didn't initiate the start offset, will seek from topic's earliest offset.");
                result.setSourcePartitionOffsetStart(KafkaClient.getEarliestOffsets(cube));
            }
        }

        final KafkaConfig kafkaConfig = KafkaConfigManager.getInstance(KylinConfig.getInstanceFromEnv()).getKafkaConfig(cube.getRootFactTable());
        final String brokers = KafkaClient.getKafkaBrokers(kafkaConfig);
        final String topic = kafkaConfig.getTopic();
        try (final KafkaConsumer consumer = KafkaClient.getKafkaConsumer(brokers, cube.getName(), null)) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            for (PartitionInfo partitionInfo : partitionInfos) {
                if (result.getSourcePartitionOffsetStart().containsKey(partitionInfo.partition()) == false) {
                    // has new partition added
                    logger.debug("has new partition added");
                    long earliest = KafkaClient.getEarliestOffset(consumer, topic, partitionInfo.partition());
                    logger.debug("new partition " + partitionInfo.partition() + " starts from " + earliest);
                    result.getSourcePartitionOffsetStart().put(partitionInfo.partition(), earliest);
                }
            }
        }

        if (result.getEndOffset() == Long.MAX_VALUE) {
            logger.debug("Seek end offsets from topic");
            Map<Integer, Long> latestOffsets = KafkaClient.getLatestOffsets(cube);
            logger.debug("The end offsets are " + latestOffsets);

            for (Integer partitionId : latestOffsets.keySet()) {
                if (result.getSourcePartitionOffsetStart().containsKey(partitionId)) {
                    if (result.getSourcePartitionOffsetStart().get(partitionId) > latestOffsets.get(partitionId)) {
                        throw new IllegalArgumentException("Partition " + partitionId + " end offset (" + latestOffsets.get(partitionId) + ") is smaller than start offset ( " + result.getSourcePartitionOffsetStart().get(partitionId) + ")");
                    }
                } else {
                    throw new IllegalStateException("New partition added in between, retry.");
                }
            }
            result.setSourcePartitionOffsetEnd(latestOffsets);
        }

        long totalStartOffset = 0, totalEndOffset = 0;
        for (Long v : result.getSourcePartitionOffsetStart().values()) {
            totalStartOffset += v;
        }
        for (Long v : result.getSourcePartitionOffsetEnd().values()) {
            totalEndOffset += v;
        }

        if (totalStartOffset > totalEndOffset) {
            throw new IllegalArgumentException("Illegal offset: start: " + totalStartOffset + ", end: " + totalEndOffset);
        }

        if (totalStartOffset == totalEndOffset) {
            throw new IllegalArgumentException("No new message comes, startOffset = endOffset:" + totalStartOffset);
        }

        result.setStartOffset(totalStartOffset);
        result.setEndOffset(totalEndOffset);

        logger.debug("parsePartitionBeforeBuild() return: " + result);
        return result;
    }

    private void checkSourceOffsets(SourcePartition srcPartition) {
        long startOffset = srcPartition.getStartOffset();
        long endOffset = srcPartition.getEndOffset();
        final Map<Integer, Long> sourcePartitionOffsetStart = srcPartition.getSourcePartitionOffsetStart();
        final Map<Integer, Long> sourcePartitionOffsetEnd = srcPartition.getSourcePartitionOffsetEnd();
        if (endOffset <= 0 || startOffset >= endOffset) {
            throw new IllegalArgumentException("'startOffset' need be smaller than 'endOffset'");
        }

        if (startOffset > 0) {
            if (sourcePartitionOffsetStart == null || sourcePartitionOffsetStart.size() == 0) {
                throw new IllegalArgumentException("When 'startOffset' is > 0, need provide each partition's start offset");
            }

            long totalOffset = 0;
            for (Long v : sourcePartitionOffsetStart.values()) {
                totalOffset += v;
            }

            if (totalOffset != startOffset) {
                throw new IllegalArgumentException("Invalid 'sourcePartitionOffsetStart', doesn't match with 'startOffset'");
            }
        }

        if (endOffset > 0 && endOffset != Long.MAX_VALUE) {
            if (sourcePartitionOffsetEnd == null || sourcePartitionOffsetEnd.size() == 0) {
                throw new IllegalArgumentException("When 'endOffset' is not Long.MAX_VALUE, need provide each partition's start offset");
            }

            long totalOffset = 0;
            for (Long v : sourcePartitionOffsetEnd.values()) {
                totalOffset += v;
            }

            if (totalOffset != endOffset) {
                throw new IllegalArgumentException("Invalid 'sourcePartitionOffsetEnd', doesn't match with 'endOffset'");
            }
        }
    }

}

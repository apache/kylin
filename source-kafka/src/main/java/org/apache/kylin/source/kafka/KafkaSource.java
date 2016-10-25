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

import com.google.common.collect.Lists;

//used by reflection
public class KafkaSource implements ISource {

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
                // from last seg's end position
                result.setSourcePartitionOffsetStart(last.getSourcePartitionOffsetEnd());
            } else if (cube.getDescriptor().getPartitionOffsetStart() != null && cube.getDescriptor().getPartitionOffsetStart().size() > 0) {
                result.setSourcePartitionOffsetStart(cube.getDescriptor().getPartitionOffsetStart());
            } else {
                // from the topic's very begining;
                result.setSourcePartitionOffsetStart(KafkaClient.getEarliestOffsets(cube));
            }
        }

        final KafkaConfig kafakaConfig = KafkaConfigManager.getInstance(cube.getConfig()).getKafkaConfig(cube.getFactTable());
        final String brokers = KafkaClient.getKafkaBrokers(kafakaConfig);
        final String topic = kafakaConfig.getTopic();
        try (final KafkaConsumer consumer = KafkaClient.getKafkaConsumer(brokers, cube.getName(), null)) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            if (partitionInfos.size() > result.getSourcePartitionOffsetStart().size()) {
                // has new partition added
                for (int x = result.getSourcePartitionOffsetStart().size(); x < partitionInfos.size(); x++) {
                    long earliest = KafkaClient.getEarliestOffset(consumer, topic, partitionInfos.get(x).partition());
                    result.getSourcePartitionOffsetStart().put(partitionInfos.get(x).partition(), earliest);
                }
            }
        }

        if (result.getEndOffset() == Long.MAX_VALUE) {
            result.setSourcePartitionOffsetEnd(KafkaClient.getCurrentOffsets(cube));
        }

        long totalStartOffset = 0, totalEndOffset = 0;
        for (Long v : result.getSourcePartitionOffsetStart().values()) {
            totalStartOffset += v;
        }
        for (Long v : result.getSourcePartitionOffsetEnd().values()) {
            totalEndOffset += v;
        }

        result.setStartOffset(totalStartOffset);
        result.setEndOffset(totalEndOffset);

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

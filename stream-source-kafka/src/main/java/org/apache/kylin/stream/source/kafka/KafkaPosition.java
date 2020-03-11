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

package org.apache.kylin.stream.source.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.kylin.stream.core.source.ISourcePosition;

import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Maps.EntryTransformer;

public class KafkaPosition implements ISourcePosition {
    private Map<Integer, Long> partitionOffsetMap = Maps.newHashMap();

    public KafkaPosition() {
    }

    public KafkaPosition(Map<Integer, Long> partitionOffsetMap) {
        this.partitionOffsetMap = partitionOffsetMap;
    }

    @Override
    public void update(IPartitionPosition point) {
        KafkaPartitionPosition kafkaPartitionPosition = (KafkaPartitionPosition) point;
        partitionOffsetMap.put(kafkaPartitionPosition.partition, kafkaPartitionPosition.offset);
    }

    @Override
    public void updateWhenPartitionNotExist(IPartitionPosition partPosition) {
        KafkaPartitionPosition kafkaPartitionPosition = (KafkaPartitionPosition) partPosition;
        if (!partitionOffsetMap.containsKey(kafkaPartitionPosition.partition)) {
            partitionOffsetMap.put(kafkaPartitionPosition.partition, kafkaPartitionPosition.offset);
        }
    }

    @Override
    public ISourcePosition advance() {
        Map<Integer, Long> newOffsetMap = Maps.newHashMap();
        for (Entry<Integer, Long> partitionOffsetEntry : partitionOffsetMap.entrySet()) {
            newOffsetMap.put(partitionOffsetEntry.getKey(), partitionOffsetEntry.getValue() + 1L);
        }
        return new KafkaPosition(newOffsetMap);
    }

    @Override
    public Map<Integer, IPartitionPosition> getPartitionPositions() {
        return Maps.transformEntries(partitionOffsetMap, new EntryTransformer<Integer, Long, IPartitionPosition>() {
            @Override
            public IPartitionPosition transformEntry(@Nullable Integer key, @Nullable Long value) {
                return new KafkaPartitionPosition(key, value);
            }
        });
    }

    public Map<Integer, Long> getPartitionOffsets() {
        return partitionOffsetMap;
    }

    @Override
    public String toString() {
        return "KafkaPosition{" +
                "partitionOffsetMap=" + partitionOffsetMap +
                '}';
    }

    @Override
    public void copy(ISourcePosition other) {
        this.partitionOffsetMap = new HashMap<>(((KafkaPosition) other).partitionOffsetMap);
    }

    public static class KafkaPartitionPosition implements IPartitionPosition {
        public int partition;
        public long offset;

        public KafkaPartitionPosition(int partition, long offset) {
            this.partition = partition;
            this.offset = offset;
        }

        @Override
        public int getPartition() {
            return partition;
        }

        @Override
        public int compareTo(IPartitionPosition o) {
            KafkaPartitionPosition kafkaPartPos = (KafkaPartitionPosition) o;
            return Long.compare(offset, kafkaPartPos.offset);
        }
    }
}

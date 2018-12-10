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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.stream.core.exception.StreamingException;
import org.apache.kylin.stream.core.source.ISourcePosition;
import org.apache.kylin.stream.core.source.ISourcePositionHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

public class KafkaPositionHandler implements ISourcePositionHandler {

    @Override
    public ISourcePosition mergePositions(Collection<ISourcePosition> positions, MergeStrategy mergeStrategy) {
        KafkaPosition result = new KafkaPosition();
        for (ISourcePosition position : positions) {
            KafkaPosition kafkaPosition = (KafkaPosition) position;
            Map<Integer, Long> partitionOffsetMap = kafkaPosition.getPartitionOffsets();
            for (Entry<Integer, Long> partOffsetEntry : partitionOffsetMap.entrySet()) {
                Long existPartOffset = result.getPartitionOffsets().get(partOffsetEntry.getKey());
                if (existPartOffset == null) {
                    result.getPartitionOffsets().put(partOffsetEntry.getKey(), partOffsetEntry.getValue());
                } else {
                    int compResult = partOffsetEntry.getValue().compareTo(existPartOffset);
                    if ((mergeStrategy == MergeStrategy.KEEP_LARGE && compResult > 0)
                            || (mergeStrategy == MergeStrategy.KEEP_SMALL && compResult < 0)
                            || (mergeStrategy == MergeStrategy.KEEP_LATEST)) {
                        result.getPartitionOffsets().put(partOffsetEntry.getKey(), partOffsetEntry.getValue());
                    }
                }
            }
        }
        return result;
    }

    @Override
    public ISourcePosition createEmptyPosition() {
        return new KafkaPosition();
    }

    @Override
    public ISourcePosition parsePosition(String positionStr) {
        try {
            Map<Integer, Long> partitionOffsetMap = JsonUtil.readValue(positionStr,
                    new TypeReference<Map<Integer, Long>>() {
                    });
            return new KafkaPosition(partitionOffsetMap);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public String serializePosition(ISourcePosition position) {
        try {
            KafkaPosition kafkaPosition = (KafkaPosition) position;
            return JsonUtil.writeValueAsString(kafkaPosition.getPartitionOffsets());
        } catch (JsonProcessingException e) {
            throw new StreamingException(e);
        }
    }

}

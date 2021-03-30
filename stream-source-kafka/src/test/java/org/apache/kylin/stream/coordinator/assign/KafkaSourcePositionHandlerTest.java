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

package org.apache.kylin.stream.coordinator.assign;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.kylin.stream.core.source.ISourcePosition;
import org.apache.kylin.stream.core.source.ISourcePosition.IPartitionPosition;
import org.apache.kylin.stream.source.kafka.KafkaPosition;
import org.apache.kylin.stream.source.kafka.KafkaPosition.KafkaPartitionPosition;
import org.apache.kylin.stream.source.kafka.KafkaPositionHandler;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class KafkaSourcePositionHandlerTest {
    KafkaPositionHandler positionHandler;

    @Before
    public void setup() {
        positionHandler = new KafkaPositionHandler();
    }

    @Test
    public void testParsePosition() throws Exception {
        String kafkaPosStr = "{\"0\":161400}";
        ISourcePosition sourcePosition = positionHandler.parsePosition(kafkaPosStr);
        Map<Integer, IPartitionPosition> partitionPositionMap = sourcePosition.getPartitionPositions();
        assertEquals(((KafkaPartitionPosition)partitionPositionMap.get(0)).offset, 161400);

        ISourcePosition newPosition = positionHandler.createEmptyPosition();
        newPosition.copy(sourcePosition);
        partitionPositionMap = newPosition.getPartitionPositions();
        assertEquals(((KafkaPartitionPosition)partitionPositionMap.get(0)).offset, 161400);
    }

    @Test
    public void testSerializePosition() throws Exception {
        Map<Integer, Long> partitionOffsetMap = Maps.newHashMap();
        partitionOffsetMap.put(0, 1000L);
        partitionOffsetMap.put(1, 1001L);
        partitionOffsetMap.put(2, 1002L);
        KafkaPosition kafkaPosition = new KafkaPosition(partitionOffsetMap);
        String posStr = positionHandler.serializePosition(kafkaPosition);
        System.out.println(posStr);
        KafkaPosition deserializePosition = (KafkaPosition) positionHandler.parsePosition(posStr);
        deserializePosition.getPartitionOffsets();

        assertEquals(deserializePosition.getPartitionOffsets(), partitionOffsetMap);
    }
}

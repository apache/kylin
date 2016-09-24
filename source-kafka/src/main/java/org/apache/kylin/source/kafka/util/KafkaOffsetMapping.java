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
package org.apache.kylin.source.kafka.util;

import com.google.common.collect.Maps;
import org.apache.kylin.cube.CubeSegment;

import java.util.Map;

/**
 */
public class KafkaOffsetMapping {

    public static final String OFFSET_START = "kafka.offset.start.";
    public static final String OFFSET_END = "kafka.offset.end.";

    /**
     * Get the start offsets for each partition from a segment
     *
     * @param segment
     * @return
     */
    public static Map<Integer, Long> parseOffsetStart(CubeSegment segment) {
        return parseOffset(segment, OFFSET_START);
    }

    /**
     * Get the end offsets for each partition from a segment
     *
     * @param segment
     * @return
     */
    public static Map<Integer, Long> parseOffsetEnd(CubeSegment segment) {
        return parseOffset(segment, OFFSET_END);
    }

    /**
     * Save the partition start offset to cube segment
     *
     * @param segment
     * @param offsetStart
     */
    public static void saveOffsetStart(CubeSegment segment, Map<Integer, Long> offsetStart) {
        long sourceOffsetStart = 0;
        for (Integer partition : offsetStart.keySet()) {
            segment.getAdditionalInfo().put(OFFSET_START + partition, String.valueOf(offsetStart.get(partition)));
            sourceOffsetStart += offsetStart.get(partition);
        }

        segment.setSourceOffsetStart(sourceOffsetStart);
    }

    /**
     * Save the partition end offset to cube segment
     *
     * @param segment
     * @param offsetEnd
     */
    public static void saveOffsetEnd(CubeSegment segment, Map<Integer, Long> offsetEnd) {
        long sourceOffsetEnd = 0;
        for (Integer partition : offsetEnd.keySet()) {
            segment.getAdditionalInfo().put(OFFSET_END + partition, String.valueOf(offsetEnd.get(partition)));
            sourceOffsetEnd += offsetEnd.get(partition);
        }

        segment.setSourceOffsetEnd(sourceOffsetEnd);
    }

    private static Map<Integer, Long> parseOffset(CubeSegment segment, String propertyPrefix) {
        final Map<Integer, Long> offsetStartMap = Maps.newHashMap();
        for (String key : segment.getAdditionalInfo().keySet()) {
            if (key.startsWith(propertyPrefix)) {
                Integer partition = Integer.valueOf(key.substring(propertyPrefix.length()));
                Long offset = Long.valueOf(segment.getAdditionalInfo().get(key));
                offsetStartMap.put(partition, offset);
            }
        }


        return offsetStartMap;
    }
}

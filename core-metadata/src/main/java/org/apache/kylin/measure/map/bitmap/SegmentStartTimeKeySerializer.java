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

package org.apache.kylin.measure.map.bitmap;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.measure.map.MapKeySerializer;

/**
 * Use the segment start time as the map key, the time unit depends on the partition columns
 * If the partition_time_column is null, the unit is day;
 *                            otherwise, the unit is second
 */
public class SegmentStartTimeKeySerializer implements MapKeySerializer<Long> {

    @Override
    public void writeKey(ByteBuffer out, Long segStartTime) {
        BytesUtil.writeVLong(segStartTime, out);
    }

    @Override
    public Long readKey(ByteBuffer in) {
        return BytesUtil.readVLong(in);
    }

    @Override
    public Long parseKey(String value) {
        return Long.parseLong(value);
    }
}

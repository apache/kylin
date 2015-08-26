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

package org.apache.kylin.metadata.measure.serializer;

import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.metadata.model.DataType;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * 
 */
public class TopNCounterSerializer extends DataTypeSerializer<TopNCounter<ByteArray>> {

    // be thread-safe and avoid repeated obj creation
    private ThreadLocal<TopNCounter<ByteArray>> current = new ThreadLocal<TopNCounter<ByteArray>>();

    @Override
    public int peekLength(ByteBuffer in) {
        return 0;
    }

    @Override
    public int maxLength() {
        return 0;
    }

    @Override
    public TopNCounter<ByteArray> valueOf(byte[] value) {
        return null;
    }

    @Override
    public void serialize(TopNCounter<ByteArray> value, ByteBuffer out) {

    }

    @Override
    public TopNCounter<ByteArray> deserialize(ByteBuffer in) {
        return null;
    }
}

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

package org.apache.kylin.metadata.measure;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;

/**
 * @author yangli9
 * 
 */
public class LongSerializer extends MeasureSerializer<LongWritable> {

    // avoid mass object creation
    LongWritable current = new LongWritable();

    @Override
    public void serialize(LongWritable value, ByteBuffer out) {
        BytesUtil.writeVLong(value.get(), out);
    }

    @Override
    public LongWritable deserialize(ByteBuffer in) {
        current.set(BytesUtil.readVLong(in));
        return current;
    }

    @Override
    public LongWritable valueOf(byte[] value) {
        if (value == null)
            current.set(0L);
        else
            current.set(Long.parseLong(Bytes.toString(value)));
        return current;
    }

}

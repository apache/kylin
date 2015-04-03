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

package org.apache.kylin.metadata.serializer;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.DataType;

/**
 * @author yangli9
 * 
 */
public class LongSerializer extends DataTypeSerializer<LongWritable> {

    // avoid mass object creation
    LongWritable current = new LongWritable();

    public LongSerializer(DataType type) {
    }

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
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        
        BytesUtil.readVLong(in);
        int len = in.position() - mark;
        
        in.position(mark);
        return len;
    }
    
    @Override
    public int maxLength() {
        return 9; // vlong: 1 + 8
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

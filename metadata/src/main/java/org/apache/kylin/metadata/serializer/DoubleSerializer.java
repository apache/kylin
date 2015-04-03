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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.kylin.metadata.model.DataType;

/**
 * @author yangli9
 * 
 */
public class DoubleSerializer extends DataTypeSerializer<DoubleWritable> {

    // avoid mass object creation
    DoubleWritable current = new DoubleWritable();

    public DoubleSerializer(DataType type) {
    }

    @Override
    public void serialize(DoubleWritable value, ByteBuffer out) {
        out.putDouble(value.get());
    }

    @Override
    public DoubleWritable deserialize(ByteBuffer in) {
        current.set(in.getDouble());
        return current;
    }

    @Override
    public int peekLength(ByteBuffer in) {
        return 8;
    }

    @Override
    public int maxLength() {
        return 8;
    }
    
    @Override
    public DoubleWritable valueOf(byte[] value) {
        if (value == null)
            current.set(0d);
        else
            current.set(Double.parseDouble(Bytes.toString(value)));
        return current;
    }

}

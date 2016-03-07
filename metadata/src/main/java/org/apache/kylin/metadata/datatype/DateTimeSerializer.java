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


package org.apache.kylin.metadata.datatype;

import java.nio.ByteBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.DateFormat;

public class DateTimeSerializer extends DataTypeSerializer<LongWritable> {

    // be thread-safe and avoid repeated obj creation
    private ThreadLocal<LongWritable> current = new ThreadLocal<LongWritable>();

    public DateTimeSerializer(DataType type) {
    }

    @Override
    public void serialize(LongWritable value, ByteBuffer out) {
        out.putLong(value.get());
    }

    private LongWritable current() {
        LongWritable l = current.get();
        if (l == null) {
            l = new LongWritable();
            current.set(l);
        }
        return l;
    }

    @Override
    public LongWritable deserialize(ByteBuffer in) {
        LongWritable l = current();
        l.set(in.getLong());
        return l;
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
    public int getStorageBytesEstimate() {
        return 8;
    }

    @Override
    public LongWritable valueOf(String str) {
        return new LongWritable(DateFormat.stringToMillis(str));
    }

}

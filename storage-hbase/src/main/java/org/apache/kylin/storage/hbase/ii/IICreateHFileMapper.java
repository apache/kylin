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

package org.apache.kylin.storage.hbase.ii;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.invertedindex.model.IIDesc;

/**
 * @author yangli9
 */
public class IICreateHFileMapper extends KylinMapper<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue> {

    long timestamp;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.bindCurrentConfiguration(context.getConfiguration());

        timestamp = System.currentTimeMillis();
    }

    @Override
    protected void map(ImmutableBytesWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {

        ByteBuffer buffer = ByteBuffer.wrap(value.get(), value.getOffset(), value.getLength());
        int totalLength = value.getLength();
        int valueLength = buffer.getInt();
        int dictionaryLength = totalLength - valueLength - 4;
        KeyValue kv = new KeyValue(key.get(), key.getOffset(), key.getLength(), //
                IIDesc.HBASE_FAMILY_BYTES, 0, IIDesc.HBASE_FAMILY_BYTES.length, //
                IIDesc.HBASE_QUALIFIER_BYTES, 0, IIDesc.HBASE_QUALIFIER_BYTES.length, //
                timestamp, Type.Put, //
                buffer.array(), buffer.position(), valueLength);

        // write value
        context.write(key, kv);

        kv = new KeyValue(key.get(), key.getOffset(), key.getLength(), //
                IIDesc.HBASE_FAMILY_BYTES, 0, IIDesc.HBASE_FAMILY_BYTES.length, //
                IIDesc.HBASE_DICTIONARY_BYTES, 0, IIDesc.HBASE_DICTIONARY_BYTES.length, //
                timestamp, Type.Put, //
                buffer.array(), buffer.position() + valueLength, dictionaryLength);

        // write dictionary
        context.write(key, kv);

    }

}

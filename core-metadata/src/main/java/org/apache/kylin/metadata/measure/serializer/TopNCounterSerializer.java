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

import com.google.common.collect.Lists;
import org.apache.kylin.common.topn.DoubleDeltaSerializer;
import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.DataType;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * 
 */
public class TopNCounterSerializer extends DataTypeSerializer<TopNCounter<ByteArray>> {

    private DoubleDeltaSerializer dds = new DoubleDeltaSerializer();

    private int precision;
    
    public TopNCounterSerializer(DataType dataType) {
        this.precision = dataType.getPrecision();
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        int capacity = in.getInt();
        int size = in.getInt();
        int keyLength = in.getInt();
        dds.deserialize(in);
        int len = in.position() - mark + keyLength * size;
        in.position(mark);
        return len;
    }

    @Override
    public int maxLength() {
        return precision * TopNCounter.EXTRA_SPACE_RATE * (4 + 8);
    }

    @Override
    public TopNCounter<ByteArray> valueOf(byte[] value) {
        ByteBuffer buffer = ByteBuffer.wrap(value);
        int sizeOfId = buffer.getInt();
        int keyEncodedValue = buffer.getInt();
        double counter = buffer.getDouble();

        ByteArray key = new ByteArray(sizeOfId);
        BytesUtil.writeUnsigned(keyEncodedValue, key.array(), 0, sizeOfId);
        
        TopNCounter<ByteArray> topNCounter = new TopNCounter<ByteArray>(precision * TopNCounter.EXTRA_SPACE_RATE);
        topNCounter.offer(key, counter);
        return topNCounter;
    }

    @Override
    public void serialize(TopNCounter<ByteArray> value, ByteBuffer out) {
        double[] counters = value.getCounters();
        List<ByteArray> items = value.getItems();
        int keyLength = items.size() > 0 ? items.get(0).length() : 0;
        out.putInt(value.getCapacity());
        out.putInt(value.size());
        out.putInt(keyLength);
        dds.serialize(counters, out);

        for (ByteArray item : items) {
            out.put(item.array());
        }
    }

    @Override
    public TopNCounter<ByteArray> deserialize(ByteBuffer in) {
        int capacity = in.getInt();
        int size = in.getInt();
        int keyLength = in.getInt();
        double[] counters = dds.deserialize(in);
        List<ByteArray> items = Lists.newArrayList();
        
        for(int i=0; i<size; i++) {
            ByteArray byteArray = new ByteArray(keyLength);
            in.get(byteArray.array());
            items.add(byteArray);
        }
        
        TopNCounter<ByteArray> counter = new TopNCounter<ByteArray>(capacity);
        counter.fromExternal(size, counters, items);
        return counter;
    }
}

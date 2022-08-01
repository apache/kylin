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

package org.apache.kylin.measure.topn;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

/**
 *
 */
public class TopNCounterSerializer extends DataTypeSerializer<TopNCounter<ByteArray>> {

    private DoubleDeltaSerializer dds = new DoubleDeltaSerializer(3);

    private int precision;

    private int scale;

    public TopNCounterSerializer(DataType dataType) {
        this.precision = dataType.getPrecision();
        this.scale = dataType.getScale();
        if (scale < 0) {
            scale = DictionaryDimEnc.MAX_ENCODING_LENGTH;
        }
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();
        @SuppressWarnings("unused")
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
        return Math.max(precision * TopNCounter.EXTRA_SPACE_RATE * (scale + 8), 1024 * 1024); // use at least 1M
    }

    @Override
    public int getStorageBytesEstimate() {
        return precision * TopNCounter.EXTRA_SPACE_RATE * (scale + 8);
    }

    @Override
    public void serialize(TopNCounter<ByteArray> value, ByteBuffer out) {
        double[] counters = value.getCounters();
        List<Counter<ByteArray>> peek = value.topK(1);
        int keyLength = peek.size() > 0 ? peek.get(0).getItem().length() : 0;
        out.putInt(value.getCapacity());
        out.putInt(value.size());
        out.putInt(keyLength);
        dds.serialize(counters, out);
        Iterator<Counter<ByteArray>> iterator = value.iterator();
        ByteArray item;
        while (iterator.hasNext()) {
            item = iterator.next().getItem();
            out.put(item.array(), item.offset(), item.length());
        }
    }

    @Override
    public TopNCounter<ByteArray> deserialize(ByteBuffer in) {
        int capacity = in.getInt();
        int size = in.getInt();
        int keyLength = in.getInt();
        double[] counters = dds.deserialize(in);

        TopNCounter<ByteArray> counter = new TopNCounter<ByteArray>(capacity);
        ByteArray byteArray;
        byte[] keyArray = new byte[size * keyLength];
        int offset = 0;
        for (int i = 0; i < size; i++) {
            in.get(keyArray, offset, keyLength);
            byteArray = new ByteArray(keyArray, offset, keyLength);
            counter.offerToHead(byteArray, counters[i]);
            offset += keyLength;
        }

        return counter;
    }

}

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

import org.apache.kylin.common.topn.TopNCounter;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.model.DataType;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * 
 */
public class TopNCounterSerializerTest {

    private static TopNCounterSerializer serializer = new TopNCounterSerializer(DataType.getInstance("topn(10)"));

    @SuppressWarnings("unchecked")
    @Test
    public void testSerialization() {
        TopNCounter<ByteArray> vs = new TopNCounter<ByteArray>(50);
        Integer[] stream = { 1, 1, 2, 9, 1, 2, 3, 7, 7, 1, 3, 1, 1 };
        for (Integer i : stream) {
            vs.offer(new ByteArray(Bytes.toBytes(i)));
        }

        ByteBuffer out = ByteBuffer.allocate(1024);
        serializer.serialize(vs, out);
        
        byte[] copyBytes = new byte[out.position()];
        System.arraycopy(out.array(), 0, copyBytes, 0, out.position());

        ByteBuffer in = ByteBuffer.wrap(copyBytes);
        TopNCounter<ByteArray> vsNew = serializer.deserialize(in);

        Assert.assertEquals(vs.toString(), vsNew.toString());

    }
    
    @Test
    public void testValueOf() {

        TopNCounter<ByteArray> origin = new TopNCounter<ByteArray>(10);
        ByteArray key = new ByteArray(1);
        ByteBuffer byteBuffer = key.asBuffer();
        BytesUtil.writeVLong(20l, byteBuffer);
        origin.offer(key, 1.0);

        byteBuffer = ByteBuffer.allocate(1024);
        byteBuffer.putInt(1);
        byteBuffer.putInt(20);
        byteBuffer.putDouble(1.0);
        TopNCounter<ByteArray> counter = serializer.valueOf(byteBuffer.array());


        Assert.assertEquals(origin.toString(), counter.toString());
    }

}

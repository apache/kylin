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

package org.apache.kylin.aggregation.topn;

import java.nio.ByteBuffer;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.measure.topn.TopNCounter;
import org.apache.kylin.measure.topn.TopNCounterSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TopNCounterSerializerTest extends LocalFileMetadataTestCase {

    private static TopNCounterSerializer serializer;

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();

        DataType.register("topn");
        serializer = new TopNCounterSerializer(DataType.getType("topn(10)"));
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

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
        // FIXME need a good unit test for valueOf()
    }
}

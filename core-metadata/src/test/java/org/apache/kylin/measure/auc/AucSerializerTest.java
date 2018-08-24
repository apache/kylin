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

package org.apache.kylin.measure.auc;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AucSerializerTest extends LocalFileMetadataTestCase {

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }


    @Test
    public void testBasic() {
        AucSerializer serializer = new AucSerializer(DataType.getType("auc"));

        List<Integer> truth = new LinkedList<>(Arrays.asList(0, 0, 1, 1));
        List<Double> pred = new LinkedList<>(Arrays.asList(0.1, 0.4, 0.35, 0.8));

        AucCounter counter = new AucCounter(truth, pred);
        double markResult = counter.auc();
        assertEquals(markResult, 0.75, 0.01);

        ByteBuffer buffer = ByteBuffer.allocateDirect(serializer.getStorageBytesEstimate());
        serializer.serialize(counter, buffer);

        buffer.flip();
        counter = serializer.deserialize(buffer);
        List<Integer> truth1 = new LinkedList<>(Arrays.asList(1, 0, 1, 1));
        List<Double> pred1 = new LinkedList<>(Arrays.asList(0.9, 0.4, 0.65, 0.8));
        AucCounter counter1 = new AucCounter(truth1, pred1);
        counter1.merge(counter);

        assertEquals(0.86, counter1.auc(), 0.01);
    }

    @Test
    public void testNull() {

        List<Integer> truth = null;
        List<Double> pred = null;

        AucCounter counter = new AucCounter(truth, pred);
        double markResult = counter.auc();
        assertEquals(markResult, -1.0, 0.01);

    }

    @Test
    public void testNan() {

        List<Integer> truth = new LinkedList<>(Arrays.asList(1, 1, 1, 1));
        List<Double> pred = new LinkedList<>(Arrays.asList(0.1, 0.4, 0.35, 0.8));

        AucCounter counter = new AucCounter(truth, pred);
        double markResult = counter.auc();
        assertEquals(markResult, -1.0, 0.01);
    }
}

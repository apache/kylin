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

package org.apache.kylin.measure.percentile;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.kylin.common.util.CleanMetadataHelper;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by dongli on 5/21/16.
 */
public class PercentileSerializerTest {

    private CleanMetadataHelper cleanMetadataHelper = null;

    @Before
    public void setUp() throws Exception {
        cleanMetadataHelper = new CleanMetadataHelper();
        cleanMetadataHelper.setUp();
    }

    @After
    public void after() throws Exception {
        cleanMetadataHelper.tearDown();
    }

    @Test
    public void testBasic() {
        PercentileSerializer serializer = new PercentileSerializer(DataType.getType("percentile(100)"));
        PercentileCounter counter = new PercentileCounter(100, 0.5);
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            counter.add(random.nextDouble());
        }
        double markResult = counter.getResultEstimate();

        ByteBuffer buffer = ByteBuffer.allocateDirect(serializer.getStorageBytesEstimate());
        serializer.serialize(counter, buffer);

        buffer.flip();
        counter = serializer.deserialize(buffer);
        PercentileCounter counter1 = new PercentileCounter(100, 0.5);
        counter1.merge(counter);

        assertEquals(markResult, counter1.getResultEstimate(), 0.01);
    }

}

/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.streaming;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import junit.framework.TestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.fail;

/**
 */
public class StreamingManagerTest extends LocalFileMetadataTestCase {

    private KylinConfig kylinConfig;
    private StreamingManager streamingManager;

    @Before
    public void before() {
        this.createTestMetadata();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        streamingManager = StreamingManager.getInstance(kylinConfig);
    }

    @After
    public void after() {
        this.cleanupTestMetadata();
    }

    @Test
    public void test() {
        assertNotNull(streamingManager.getStreamingConfig("kafka_test"));
    }

    @Test
    public void testOffset() {
        final String streaming = "kafka_test";
        final int partition = 0;
        assertEquals(0, streamingManager.getOffset(streaming, partition));

        try {
            updateOffsetAndCompare(streaming, partition, -1);
            fail("offset cannot be smaller than 0");
        } catch (IllegalArgumentException e) {
        }
        updateOffsetAndCompare(streaming, partition, 1000);
        updateOffsetAndCompare(streaming, partition, 800);
        updateOffsetAndCompare(streaming, partition, 2000);
    }

    @Test
    public void testMultiOffset() {
        final String streaming = "kafka_multi_test";
        List<Integer> partitions = Lists.newArrayList(Lists.asList(0, 1, new Integer[]{2, 3}));
        assertEquals(0, streamingManager.getOffset("kafka_multi_test", partitions).size());

        for (int i = 0; i < 10; i++) {
            updateOffsetAndCompare(streaming, generateRandomOffset(partitions));
        }
    }

    private HashMap<Integer, Long> generateRandomOffset(List<Integer> partitions) {
        final HashMap<Integer, Long> result = Maps.newHashMap();
        final Random random = new Random();
        for (Integer partition : partitions) {
            result.put(partition, random.nextLong());
        }
        return result;
    }

    private void updateOffsetAndCompare(String streaming, int partition, long offset) {
        streamingManager.updateOffset(streaming, partition, offset);
        assertEquals(offset, streamingManager.getOffset(streaming, partition));
    }

    private void updateOffsetAndCompare(String streaming, HashMap<Integer, Long> offset) {
        streamingManager.updateOffset(streaming, offset);
        final Map<Integer, Long> result = streamingManager.getOffset(streaming, Lists.newLinkedList(offset.keySet()));
        System.out.println(result);
        assertEquals(offset.size(), result.size());
        for (Integer partition : result.keySet()) {
            assertEquals(offset.get(partition), result.get(partition));
        }
    }
}

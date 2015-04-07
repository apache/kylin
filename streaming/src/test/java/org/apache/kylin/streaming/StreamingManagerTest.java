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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Created by qianzhou on 3/25/15.
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
        assertNotNull(streamingManager.getKafkaConfig("kafka_test"));
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

    private void updateOffsetAndCompare(String streaming, int partition, long offset) {
        streamingManager.updateOffset(streaming, partition, offset);
        assertEquals(offset, streamingManager.getOffset(streaming, partition));
    }
}

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

package org.apache.kylin.metrics.lib.impl;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metrics.lib.Record;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BlockingReservoirTest extends LocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testUpdate() {
        BlockingReservoir reservoir = new BlockingReservoir();

        Record record = new RecordEvent("TEST");
        reservoir.update(record);
        assertEquals(0, reservoir.size());

        reservoir.start();
        reservoir.update(record);
        assertEquals(1, reservoir.size());

        reservoir.stop();
        assertEquals(0, reservoir.size());
    }

    @Test
    public void testBatchSize() {
        BlockingReservoir reservoir = new BlockingReservoir(5, 12);
        reservoir.setReady();

        for (int i = 0; i < 30; i++) {
            Record record = new RecordEvent("TEST" + i);
            reservoir.update(record);
        }
        reservoir.notifyUpdate();
        Assert.assertEquals(18, reservoir.size());

        reservoir.notifyUpdate();
        Assert.assertEquals(6, reservoir.size());

        reservoir.notifyUpdate();
        Assert.assertEquals(0, reservoir.size());
    }
}

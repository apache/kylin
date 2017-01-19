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

package org.apache.kylin.storage.hbase.steps;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author George Song (ysong1)
 * 
 */
public class CreateHTableTest extends LocalFileMetadataTestCase {

    private Configuration conf;

    @Before
    public void setup() throws Exception {
        conf = HadoopUtil.getCurrentConfiguration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.application.framework.path", "");

        this.createTestMetadata();

    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetSplits() throws IllegalArgumentException, Exception {
        CreateHTableJob c = new CreateHTableJob();

        String input = "src/test/resources/partition_list/part-r-00000";

        byte[][] splits = c.getRegionSplits(conf, new Path(input));

        assertEquals(497, splits.length);
        assertArrayEquals(new byte[] { 0, 0, 0, 0, 0, 0, 15, -1, 11, 51, -45, 2 }, splits[0]);
        assertArrayEquals(new byte[] { 0, 0, 0, 0, 0, 3, -1, -1, -54, -61, 109, -44, 1 }, splits[496]);
    }

}

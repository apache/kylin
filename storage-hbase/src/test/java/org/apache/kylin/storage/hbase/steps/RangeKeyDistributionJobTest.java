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

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RangeKeyDistributionJobTest extends LocalFileMetadataTestCase {

    private Configuration conf;

    @Before
    public void setup() throws Exception {
        conf = HadoopUtil.getCurrentConfiguration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.application.framework.path", "");

        // for local runner out-of-memory issue
        conf.set("mapreduce.task.io.sort.mb", "10");
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testJob() throws Exception {
        String input = "src/test/resources/data/base_cuboid/,src/test/resources/data/8d_cuboid/";
        String output = "target/test-output/key_distribution_range/";
        String jobname = "calculate_splits";
        String cubename = "test_kylin_cube_with_slr_ready";

        FileUtil.fullyDelete(new File(output));

        String[] args = { "-input", input, "-output", output, "-jobname", jobname, "-cubename", cubename };
        assertEquals("Job failed", 0, ToolRunner.run(conf, new RangeKeyDistributionJob(), args));
    }

}

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

package org.apache.kylin.engine.mr.steps;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("as it require specify data input")
public class NDCuboidJobTest extends LocalFileMetadataTestCase {

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
    public void testsJob8D() throws Exception {
        String input = "src/test/resources/data/base_cuboid/";
        String output = "target/test-output/8d_cuboid";
        String cubeName = "test_kylin_cube_with_slr_1_new_segment";
        String segmentName = "20130331080000_20131212080000";
        String jobname = "8d_cuboid";
        String level = "1";

        FileUtil.fullyDelete(new File(output));

        String[] args = { "-input", input, "-cubename", cubeName, "-segmentname", segmentName, "-output", output, "-jobname", jobname, "-level", level };
        assertEquals("Job failed", 0, ToolRunner.run(conf, new NDCuboidJob(), args));
    }

    @Test
    public void testJob7D() throws Exception {
        final String input = "src/test/resources/data/8d_cuboid/";
        final String output = "target/test-output/7d_cuboid";
        final String cubeName = "test_kylin_cube_with_slr_1_new_segment";
        String segmentName = "20130331080000_20131212080000";
        String jobname = "7d_cuboid";
        String level = "2";

        FileUtil.fullyDelete(new File(output));

        String[] args = { "-input", input, "-cubename", cubeName, "-segmentname", segmentName, "-output", output, "-jobname", jobname, "-level", level };
        assertEquals("Job failed", 0, ToolRunner.run(conf, new NDCuboidJob(), args));
    }
}

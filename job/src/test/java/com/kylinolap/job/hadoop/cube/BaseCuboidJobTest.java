/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.hadoop.cube;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.kylinolap.common.util.AbstractKylinTestCase;
import com.kylinolap.common.util.LocalFileMetadataTestCase;

/**
 * @author George Song (ysong1)
 * 
 */
public class BaseCuboidJobTest extends LocalFileMetadataTestCase {

    private Configuration conf;

    @Before
    public void setup() throws Exception {
        conf = new Configuration();
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
        String input = "src/test/resources/data/flat_table/";
        String output = "target/test-output/base_cuboid/";
        String cubeName = "test_kylin_cube_with_slr_1_new_segment";
        String segmentName = "20130331080000_20131212080000";
        String jobname = "base_cuboid_job";
        String level = "0";
        FileUtil.fullyDelete(new File(output));

        String[] args = { "-input", input, "-cubename", cubeName, "-segmentname", segmentName, "-output", output, "-jobname", jobname, "-level", level };
        assertEquals("Job failed", 0, ToolRunner.run(conf, new BaseCuboidJob(), args));
    }

    @Test
    public void testJobWithBadParas() throws Exception {

        final String input = "src/test/resources/data/flat_table/";
        final String output = "target/test-output/base_cuboid/";
        final String metadata = AbstractKylinTestCase.LOCALMETA_TEST_DATA;

        FileUtil.fullyDelete(new File(output));

        String[] args = { "-input", input, "-output", output, "-metadata", metadata };
        assertEquals(2, ToolRunner.run(conf, new BaseCuboidJob(), args));
    }
}

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

package com.kylinolap.job.tools;

import com.kylinolap.job.hadoop.cardinality.HiveColumnCardinalityJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author ysong1
 */
public class ColumnCardinalityJobTest {

    private Configuration conf;

    @Before
    public void setup() throws IOException {
        conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
    }

    @Test
    @Ignore
    public void testJob() throws Exception {
        final String input = "src/test/resources/data/test_cal_dt/";
        final String output = "target/test-output/column-cardinality/";

        FileUtil.fullyDelete(new File(output));

        String[] args = {"-input", input, "-output", output, "-cols", "1,2,3,4,5,6,9,0"};
        assertEquals("Job failed", 0, ToolRunner.run(new HiveColumnCardinalityJob(), args));
    }

}

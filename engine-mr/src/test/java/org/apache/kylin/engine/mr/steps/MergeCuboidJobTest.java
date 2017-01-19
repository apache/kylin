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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("broken test, mergedCubeSegment in MergeCuboidMapper is not available. Besides, its input is difficult to maintain")
public class MergeCuboidJobTest extends LocalFileMetadataTestCase {

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
    public void test() throws Exception {
        // String input =
        // "src/test/resources/data/base_cuboid,src/test/resources/data/6d_cuboid";
        String output = "target/test-output/merged_cuboid";
        String cubeName = "test_kylin_cube_with_slr_ready";
        String jobname = "merge_cuboid";

        File baseFolder = File.createTempFile("kylin-f24668f6-dcff-4cb6-a89b-77f1119df8fa-", "base");
        FileUtils.forceDelete(baseFolder);
        baseFolder.mkdir();
        FileUtils.copyDirectory(new File("src/test/resources/data/base_cuboid"), baseFolder);
        FileUtils.forceDeleteOnExit(baseFolder);

        File eightFoler = File.createTempFile("kylin-f24668f6-dcff-4cb6-a89b-77f1119df8fa-", "8d");
        FileUtils.forceDelete(eightFoler);
        eightFoler.mkdir();
        FileUtils.copyDirectory(new File("src/test/resources/data/base_cuboid"), eightFoler);
        FileUtils.forceDeleteOnExit(eightFoler);

        FileUtil.fullyDelete(new File(output));

        // CubeManager cubeManager =
        // CubeManager.getInstanceFromEnv(getTestConfig());

        String[] args = { "-input", baseFolder.getAbsolutePath() + "," + eightFoler.getAbsolutePath(), "-cubename", cubeName, "-segmentname", "20130331080000_20131212080000", "-output", output, "-jobname", jobname };
        assertEquals("Job failed", 0, ToolRunner.run(conf, new MergeCuboidJob(), args));

    }

}

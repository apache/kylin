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

package org.apache.kylin.job.hadoop.cube;

///*
// * Copyright 2013-2014 eBay Software Foundation
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.kylin.index.cube;
//
//import static org.junit.Assert.*;
//
//import java.io.File;
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileUtil;
//import org.apache.hadoop.util.ToolRunner;
//import org.junit.Before;
//import org.junit.Test;
//
//import org.apache.kylin.metadata.MetadataManager;
//
///**
// * @author xjiang
// *
// */
//public class KeyDistributionJobTest {
//
//    private Configuration conf;
//
//    @Before
//    public void setup() throws IOException {
//        conf = new Configuration();
//        conf.set("fs.default.name", "file:///");
//        conf.set("mapred.job.tracker", "local");
//    }
//
//    @Test
//    public void testJob() throws Exception {
//        final String input = "src/test/resources/data/base_cuboid/,src/test/resources/data/6d_cuboid/";
//        final String output = "target/test-output/key_distribution/";
//        final String cubeName = "test_kylin_cube_with_slr";
//        final String metadata = MetadataManager.getMetadataUrlFromEnv();
//
//        FileUtil.fullyDelete(new File(output));
//
//        String[] args =
//                { "-input", input, "-cubename", cubeName, "-output", output, "-metadata", metadata,
//                        "-columnpercentage", "50", "-splitnumber", "10" };
//        assertEquals("Job failed", 0, ToolRunner.run(new KeyDistributionJob(), args));
//    }
//
// }

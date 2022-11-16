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

package org.apache.kylin.engine.spark.model;


import lombok.val;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.kylin.common.KylinConfigBase.WRITING_CLUSTER_WORKING_DIR;

public class SegmentFlatTableDescTest extends NLocalFileMetadataTestCase {

    SegmentFlatTableDesc segmentFlatTableDesc;
    KylinConfig testConfig;

    @Before
    public void setup() throws IOException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException, InstantiationException {
        createTestMetadata();
        testConfig = getTestConfig();
        testConfig.setProperty("kylin.query.engine.sparder-additional-files", "../../../build/conf/spark-executor-log4j.xml");
        val dfMgr = NDataflowManager.getInstance(testConfig, "default");
        val df = dfMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
        val seg = df.getSegments().getFirstSegment();
        val toBuildTree = new AdaptiveSpanningTree(testConfig,
                new AdaptiveSpanningTree.AdaptiveTreeBuilder(seg, seg.getIndexPlan().getAllLayouts()));
        segmentFlatTableDesc = new SegmentFlatTableDesc(testConfig, seg, toBuildTree);
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBuildFilesSeparationPathExists() throws IOException {
        Path flatTablePath = new Path("/flat_table");
        // isBuildFilesSeparationEnabled false
        Assert.assertFalse(segmentFlatTableDesc.buildFilesSeparationPathExists(flatTablePath));
        // isBuildFilesSeparationEnabled true
        testConfig.setProperty("kylin.engine.submit-hadoop-conf-dir", "/kylin");
        testConfig.setProperty(WRITING_CLUSTER_WORKING_DIR, "/flat_table");
        Assert.assertFalse(segmentFlatTableDesc.buildFilesSeparationPathExists(flatTablePath));
        // reset
        testConfig.setProperty("kylin.engine.submit-hadoop-conf-dir", "");
        testConfig.setProperty(WRITING_CLUSTER_WORKING_DIR, "");
    }
}
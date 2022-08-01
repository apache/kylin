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

package org.apache.kylin.metadata.cube.model;

import java.io.IOException;

import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;
import lombok.var;

public class NDataflowTest extends NLocalFileMetadataTestCase {
    private String projectDefault = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasic() throws IOException {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        NDataflow df = dsMgr.getDataflowByModelAlias("nmodel_basic");
        IndexPlan cube = df.getIndexPlan();

        Assert.assertNotNull(df);
        Assert.assertNotNull(cube);
        Assert.assertSame(getTestConfig(), df.getConfig().base());
        Assert.assertEquals(getTestConfig(), df.getConfig());
        Assert.assertEquals(getTestConfig().hashCode(), df.getConfig().hashCode());
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());

        Segments<NDataSegment> segments = df.getSegments();
        Assert.assertEquals(1, segments.size());
        for (NDataSegment seg : segments) {
            Assert.assertNotNull(seg);
            Assert.assertNotNull(seg.getName());
        }
    }

    @Test
    public void getConfig() {
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), projectDefault);
        val indexPlan = indexPlanManager.getIndexPlanByModelAlias("nmodel_basic");
        val indexPlanConfig = indexPlan.getConfig();
        val dsMgr = NDataflowManager.getInstance(getTestConfig(), projectDefault);
        val df = dsMgr.getDataflowByModelAlias("nmodel_basic");
        var config = df.getConfig();
        Assert.assertEquals(indexPlanConfig.base(), config.base());
        Assert.assertEquals(2, config.getExtendedOverrides().size());

        indexPlanManager.updateIndexPlan("89af4ee2-2cdb-4b07-b39e-4c29856309aa", copyForWrite -> {
            copyForWrite.getOverrideProps().put("test", "test");
        });

        config = df.getConfig();
        Assert.assertEquals(indexPlanConfig.base(), config.base());
        Assert.assertEquals(3, config.getExtendedOverrides().size());
    }

    @Test
    public void testCollectPrecalculationResource() {
        val dsMgr = NDataflowManager.getInstance(getTestConfig(), "cc_test");
        val df = dsMgr.getDataflowByModelAlias("test_model");
        val strings = df.collectPrecalculationResource();
        Assert.assertEquals(9, strings.size());

        Assert.assertTrue(strings.stream().anyMatch(path -> path.equals("/_global/project/cc_test.json")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/model_desc/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/index_plan/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/dataflow/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/dataflow_details/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/table/")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/cc_test/table_exd/")));
    }

    @Test
    public void testCollectPrecalculationResource_Streaming() {
        val dsMgr = NDataflowManager.getInstance(getTestConfig(), "streaming_test");
        val df = dsMgr.getDataflow("4965c827-fbb4-4ea1-a744-3f341a3b030d");
        val strings = df.collectPrecalculationResource();
        Assert.assertEquals(7, strings.size());

        Assert.assertTrue(strings.stream()
                .anyMatch(path -> path.equals("/streaming_test/dataflow/4965c827-fbb4-4ea1-a744-3f341a3b030d.json")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith(
                "/streaming_test/dataflow_details/4965c827-fbb4-4ea1-a744-3f341a3b030d/3e560d22-b749-48c3-9f64-d4230207f120.json")));
        Assert.assertTrue(strings.stream().anyMatch(
                path -> path.startsWith("/streaming_test/index_plan/4965c827-fbb4-4ea1-a744-3f341a3b030d.json")));
        Assert.assertTrue(strings.stream().anyMatch(path -> path.startsWith("/_global/project/streaming_test.json")));
        Assert.assertTrue(strings.stream().anyMatch(
                path -> path.startsWith("/streaming_test/model_desc/4965c827-fbb4-4ea1-a744-3f341a3b030d.json")));
        Assert.assertTrue(
                strings.stream().anyMatch(path -> path.startsWith("/streaming_test/table/DEFAULT.SSB_STREAMING.json")));
        Assert.assertTrue(
                strings.stream().anyMatch(path -> path.startsWith("/streaming_test/kafka/DEFAULT.SSB_STREAMING.json")));
    }
}

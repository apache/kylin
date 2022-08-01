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
package org.apache.kylin.streaming.jobs;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StreamingTestConstant;
import org.apache.kylin.metadata.cube.model.NCubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.streaming.CreateStreamingFlatTable;
import org.apache.kylin.streaming.app.StreamingEntry;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import lombok.val;

public class CreateStreamingFlatTableTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "511a9163-7888-4a60-aa24-ae735937cc87";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGenerateStreamingDataset() {
        val config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.streaming.kafka-conf.maxOffsetsPerTrigger", "100");
        config.setProperty("kylin.streaming.kafka-conf.security.protocol", "SASL_PLAINTEXT");
        config.setProperty("kylin.streaming.kafka-conf.sasl.mechanism", "PLAIN");
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());

        val args = new String[] { PROJECT, DATAFLOW_ID, "5", "10 seconds" };
        val entry = Mockito.spy(new StreamingEntry());
        entry.setSparkSession(createSparkSession());
        val dfMgr = NDataflowManager.getInstance(config, PROJECT);
        val dataflow = dfMgr.getDataflow(DATAFLOW_ID);
        val flatTableDesc = new NCubeJoinedFlatTableDesc(dataflow.getIndexPlan());

        val seg = NDataSegment.empty();
        seg.setId("test-1234");

        val steamingFlatTable = CreateStreamingFlatTable.apply(flatTableDesc, seg, entry.createSpanningTree(dataflow),
                entry.getSparkSession(), null, "LO_PARTITIONCOLUMN", null);

        val ds = steamingFlatTable.generateStreamingDataset(config);
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        Assert.assertEquals(4, steamingFlatTable.lookupTablesGlobal().size());
        Assert.assertEquals(-1, steamingFlatTable.tableRefreshInterval());
        Assert.assertFalse(steamingFlatTable.shouldRefreshTable());
        Assert.assertEquals(DATAFLOW_ID, steamingFlatTable.model().getId());
        Assert.assertNotNull(ds);
        Assert.assertEquals(60, ds.encoder().schema().size());

    }
}

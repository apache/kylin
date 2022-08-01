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
 
package org.apache.kylin.kafka;

import static org.apache.kylin.metadata.model.NTableMetadataManager.getInstance;

import java.util.Collections;
import java.util.HashMap;

import org.apache.kylin.common.StreamingTestConstant;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.source.kafka.KafkaExplorer;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import lombok.val;
import lombok.var;

public class NSparkKafkaSourceTest extends StreamingTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String PROJECT = "streaming_test";
    private static String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetSourceMetadataExplorer() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        val kafkaExplorer = source.getSourceMetadataExplorer();
        Assert.assertTrue(kafkaExplorer instanceof KafkaExplorer);
        Assert.assertTrue(kafkaExplorer.checkTablesAccess(Collections.emptySet()));
    }

    @Test
    public void testAdaptToBuildEngine() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(false);
        Assert.assertFalse(source.enableMemoryStream());
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val ss = createSparkSession();
        val tableMetadataManager = getInstance(getTestConfig(), PROJECT);
        val tableDesc = tableMetadataManager.getTableDesc("SSB.P_LINEORDER");
        var engineAdapter = SourceFactory.createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class);
        var ds = engineAdapter.getSourceData(tableDesc, ss, new HashMap<>());
        Assert.assertEquals(1, ds.count());

        thrown.expect(UnsupportedOperationException.class);
        source.createReadableTable(tableDesc);
    }

    @Test
    public void testAdaptToBuildEngine1() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        source.enableMemoryStream(true);
        Assert.assertTrue(source.enableMemoryStream());
        source.post(StreamingTestConstant.KAP_SSB_STREAMING_JSON_FILE());
        val tableMetadataManager = getInstance(getTestConfig(), PROJECT);
        val tableDesc = tableMetadataManager.getTableDesc("SSB.P_LINEORDER");

        val engineAdapter = SourceFactory.createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class);
        val ss = createSparkSession();
        var ds = engineAdapter.getSourceData(tableDesc, ss, new HashMap<>());

        Assert.assertEquals("memory", ds.logicalPlan().toString());
    }

    @Test
    public void testCreateReadableTable() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        thrown.expect(UnsupportedOperationException.class);
        source.createReadableTable(null);
    }

    @Test
    public void testEnrichSourcePartitionBeforeBuild() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        thrown.expect(UnsupportedOperationException.class);
        source.enrichSourcePartitionBeforeBuild(null, null);
    }

    @Test
    public void testGetSampleDataDeployer() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        thrown.expect(UnsupportedOperationException.class);
        source.getSampleDataDeployer();
    }

    @Test
    public void testGetSegmentRange() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        val seg = (SegmentRange.KafkaOffsetPartitionedSegmentRange) source.getSegmentRange("1234", "5678");
        Assert.assertTrue(seg instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange);
        Assert.assertEquals(1234L, seg.getStart().longValue());
        Assert.assertEquals(5678L, seg.getEnd().longValue());

        val seg1 = (SegmentRange.KafkaOffsetPartitionedSegmentRange) source.getSegmentRange("", "");
        Assert.assertEquals(0L, seg1.getStart().longValue());
        Assert.assertEquals(Long.MAX_VALUE, seg1.getEnd().longValue());
    }

    @Test
    public void testSupportBuildSnapShotByPartition() {
        val config = getTestConfig();
        val source = createSparkKafkaSource(config);
        Assert.assertTrue(source.supportBuildSnapShotByPartition());
    }
}

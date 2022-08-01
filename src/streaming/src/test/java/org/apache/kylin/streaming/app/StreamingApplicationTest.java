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
package org.apache.kylin.streaming.app;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.util.StreamingTestCase;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingApplicationTest extends StreamingTestCase {

    private static final String PROJECT = "streaming_test";
    private static final String DATAFLOW_ID = "e78a89dd-847f-4574-8afa-8768b4228b73";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testExecute_PrepareBeforeExecute() {
        val entry = Mockito.spy(new StreamingEntry());
        val args = new String[] { PROJECT, DATAFLOW_ID, "1", "", "xx" };
        val sparkConf = KylinBuildEnv.getOrCreate(getTestConfig()).sparkConf();
        Mockito.doNothing().when(entry).getOrCreateSparkSession(sparkConf);
        Mockito.doNothing().when(entry).doExecute();
        Mockito.doReturn(123).when(entry).reportApplicationInfo();
        entry.execute(args);
    }

    @Test
    public void testExecute_DoExecute() {
        val entry = Mockito.spy(new StreamingEntry());
        val args = new String[] { PROJECT, DATAFLOW_ID, "1", "", "xx" };
        val sparkConf = KylinBuildEnv.getOrCreate(getTestConfig()).sparkConf();
        Mockito.doNothing().when(entry).getOrCreateSparkSession(sparkConf);
        Mockito.doNothing().when(entry).doExecute();
        Mockito.doReturn(123).when(entry).reportApplicationInfo();
        entry.execute(args);
    }

    @Test
    public void testSystemExit_False() {
        overwriteSystemProp("streaming.local", "true");
        val entry = Mockito.spy(new StreamingEntry());
        Mockito.doReturn(false).when(entry).isJobOnCluster();
        entry.systemExit(0);
    }

    @Test
    public void testIsJobOnCluster_True() {
        overwriteSystemProp("streaming.local", "false");
        getTestConfig().setProperty("kylin.env", "PROD");
        val entry = Mockito.spy(new StreamingEntry());
        Assert.assertFalse(getTestConfig().isUTEnv());
        Assert.assertFalse(StreamingUtils.isLocalMode());
        Assert.assertTrue(entry.isJobOnCluster());
    }

    @Test
    public void testIsJobOnCluster_False() {

        {
            overwriteSystemProp("streaming.local", "false");
            val entry = Mockito.spy(new StreamingEntry());
            Assert.assertTrue(getTestConfig().isUTEnv());
            Assert.assertFalse(StreamingUtils.isLocalMode());
            Assert.assertFalse(entry.isJobOnCluster());
        }

        {
            overwriteSystemProp("streaming.local", "true");
            val entry = Mockito.spy(new StreamingEntry());
            getTestConfig().setProperty("kylin.env", "PROD");
            Assert.assertFalse(getTestConfig().isUTEnv());
            Assert.assertTrue(StreamingUtils.isLocalMode());
            Assert.assertFalse(entry.isJobOnCluster());
        }
    }

    @Test
    public void testCloseSparkSession_True() {
        overwriteSystemProp("streaming.local", "false");
        val entry = Mockito.spy(new StreamingEntry());
        entry.setSparkSession(createSparkSession());
        Assert.assertFalse(entry.getSparkSession().sparkContext().isStopped());
        entry.closeSparkSession();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> entry.getSparkSession().sparkContext().isStopped());
    }

    @Test
    public void testCloseSparkSession_False() {
        overwriteSystemProp("streaming.local", "true");
        val entry = Mockito.spy(new StreamingEntry());
        entry.setSparkSession(createSparkSession());
        Assert.assertFalse(entry.getSparkSession().sparkContext().isStopped());
        entry.closeSparkSession();

        Awaitility.await().atMost(30, TimeUnit.SECONDS)
                .until(() -> !entry.getSparkSession().sparkContext().isStopped());

    }

    @Test
    public void testCloseSparkSession_AlreadyStop() {
        overwriteSystemProp("streaming.local", "false");
        val entry = Mockito.spy(new StreamingEntry());
        val ss = createSparkSession();
        entry.setSparkSession(ss);
        ss.stop();
        Assert.assertTrue(entry.getSparkSession().sparkContext().isStopped());
        entry.closeSparkSession();

        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> entry.getSparkSession().sparkContext().isStopped());

    }

    @Test
    public void testStartJobExecutionIdCheckThread_NotRunning() {
        val entry = Mockito.spy(new StreamingEntry());

        Mockito.doReturn(false).when(entry).isRunning();

        Assert.assertFalse(entry.isRunning());
        entry.startJobExecutionIdCheckThread();

        Awaitility.waitAtMost(3, TimeUnit.SECONDS).until(() -> !entry.isRunning());
    }

    @Test
    public void testStartJobExecutionIdCheckThread_Exception() {
        val entry = Mockito.spy(new StreamingEntry());
        Mockito.doReturn(true).when(entry).isRunning();
        Assert.assertTrue(entry.isRunning());
        entry.startJobExecutionIdCheckThread();

        Awaitility.waitAtMost(3, TimeUnit.SECONDS).until(() -> entry.isRunning());
    }

}

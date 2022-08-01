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

package org.apache.kylin.engine.spark.utils;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cluster.AvailableResource;
import org.apache.kylin.cluster.ResourceInfo;
import org.apache.kylin.cluster.YarnClusterManager;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.spark.SparkConf;
import org.apache.spark.conf.rule.ExecutorInstancesRule;
import org.apache.spark.conf.rule.YarnConfRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

public class SparkConfHelperTest extends NLocalFileMetadataTestCase {

    private final YarnClusterManager clusterManager = mock(YarnClusterManager.class);

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        KylinBuildEnv.getOrCreate(KylinConfig.getInstanceFromEnv());
        Mockito.when(clusterManager.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(100, 100), new ResourceInfo(60480, 100)));
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testOneGradeWhenLessTHan1GB() throws JsonProcessingException {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.engine.spark.build-conf-extra-rules", "org.apache.spark.conf.rule.YarnConfRule");
        SparkConfHelper helper = new SparkConfHelper();
        helper.setClusterManager(clusterManager);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, "1b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "1");
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "default");
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("1GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("1", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("512MB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("2", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
        cleanSparkConfHelper(helper);
        helper.setConf(SparkConfHelper.COUNT_DISTICT, "true");
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.set(0, new CompareTuple("1GB", SparkConfHelper.EXECUTOR_MEMORY));
        compareTuples.set(1, new CompareTuple("5", SparkConfHelper.EXECUTOR_CORES));
        compareTuples.set(2, new CompareTuple("1GB", SparkConfHelper.EXECUTOR_OVERHEAD));
        compareConf(compareTuples, sparkConf);

    }

    @Test
    public void testTwoGradeWhenLessTHan10GB() throws JsonProcessingException {
        SparkConfHelper helper = new SparkConfHelper();
        helper.setClusterManager(clusterManager);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 8L * 1024 * 1024 * 1024 + "b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "1");
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "default");
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("4GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("1GB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("256", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
        helper.setConf(SparkConfHelper.COUNT_DISTICT, "true");
        cleanSparkConfHelper(helper);
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.set(0, new CompareTuple("10GB", SparkConfHelper.EXECUTOR_MEMORY));
        compareTuples.set(2, new CompareTuple("2GB", SparkConfHelper.EXECUTOR_OVERHEAD));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testThreeGradeWhenLessHan100GB() throws JsonProcessingException {
        SparkConfHelper helper = new SparkConfHelper();
        helper.setClusterManager(clusterManager);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 50L * 1024 * 1024 * 1024 + "b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "1");
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "default");
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("10GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("2GB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("1600", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
        helper.setConf(SparkConfHelper.COUNT_DISTICT, "true");
        cleanSparkConfHelper(helper);
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.set(0, new CompareTuple("16GB", SparkConfHelper.EXECUTOR_MEMORY));
        compareTuples.set(2, new CompareTuple("4GB", SparkConfHelper.EXECUTOR_OVERHEAD));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testFourGradeWhenGreaterThanOrEqual100GB() throws JsonProcessingException {
        SparkConfHelper helper = new SparkConfHelper();
        helper.setClusterManager(clusterManager);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 100L * 1024 * 1024 * 1024 + "b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "1");
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "default");
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("16GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("4GB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("3200", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);
        helper.setConf(SparkConfHelper.COUNT_DISTICT, "true");
        cleanSparkConfHelper(helper);
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.set(0, new CompareTuple("20GB", SparkConfHelper.EXECUTOR_MEMORY));
        compareTuples.set(2, new CompareTuple("6GB", SparkConfHelper.EXECUTOR_OVERHEAD));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testUserDefinedSparkConf() throws JsonProcessingException {
        YarnClusterManager clusterManager = mock(YarnClusterManager.class);
        Mockito.when(clusterManager.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(10240, 100), new ResourceInfo(60480, 100)));
        overwriteSystemProp("kylin.engine.base-executor-instance", "1");
        SparkConfHelper helper = new SparkConfHelper();
        helper.setClusterManager(clusterManager);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, "1b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "500");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "1");
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "default");
        helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, "4GB");
        helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, "512MB");
        helper.setConf(SparkConfHelper.EXECUTOR_CORES, "3");
        helper.setConf(SparkConfHelper.SHUFFLE_PARTITIONS, "10");
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("4GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("3", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("512MB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple("2", SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("10", SparkConfHelper.SHUFFLE_PARTITIONS));
        compareConf(compareTuples, sparkConf);

        // test spark.cores.max for Cloud
        overwriteSystemProp("kylin.env.channel", "cloud");
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.add(new CompareTuple("6", SparkConfHelper.MAX_CORES));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testUserDefinedSparkConf_Cloud() throws JsonProcessingException {
        overwriteSystemProp("kylin.env.channel", "cloud");
        SparkConfHelper helper = new SparkConfHelper();
        helper.setClusterManager(clusterManager);
        helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, "4GB");
        helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, "512MB");
        helper.setConf(SparkConfHelper.EXECUTOR_CORES, "3");
        helper.setConf(SparkConfHelper.SHUFFLE_PARTITIONS, "10");
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, "5");
        helper.setConf(SparkConfHelper.MAX_CORES, "15");
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        ArrayList<CompareTuple> compareTuples = Lists.newArrayList(
                new CompareTuple("4GB", SparkConfHelper.EXECUTOR_MEMORY),
                new CompareTuple("3", SparkConfHelper.EXECUTOR_CORES),
                new CompareTuple("512MB", SparkConfHelper.EXECUTOR_OVERHEAD),
                new CompareTuple("5", SparkConfHelper.EXECUTOR_INSTANCES),
                new CompareTuple("10", SparkConfHelper.SHUFFLE_PARTITIONS),
                new CompareTuple("15", SparkConfHelper.MAX_CORES));
        compareConf(compareTuples, sparkConf);
    }

    private void compareConf(List<CompareTuple> tuples, SparkConf conf) {
        for (CompareTuple tuple : tuples) {
            Assert.assertEquals(tuple.expect_Value, conf.get(tuple.key));
        }
    }

    @Test
    public void testComputeExecutorInstanceSize() {
        ExecutorInstancesRule executorInstancesRule = new ExecutorInstancesRule();
        Assert.assertEquals(5, executorInstancesRule.calculateExecutorInstanceSizeByLayoutSize(10));
        Assert.assertEquals(10, executorInstancesRule.calculateExecutorInstanceSizeByLayoutSize(100));
        Assert.assertEquals(10, executorInstancesRule.calculateExecutorInstanceSizeByLayoutSize(300));
        Assert.assertEquals(15, executorInstancesRule.calculateExecutorInstanceSizeByLayoutSize(600));
        Assert.assertEquals(20, executorInstancesRule.calculateExecutorInstanceSizeByLayoutSize(1000));
        Assert.assertEquals(20, executorInstancesRule.calculateExecutorInstanceSizeByLayoutSize(1200));
    }

    /**
     *  v1 = kylin.engine.base-executor-instance
     *  v2 = Calculate the number of instances based on layout size. See the calculation rules.
     *     kylin.engine.executor-instance-strategy = "100,2,500,3,1000,4"
     *     if SparkConfHelper.LAYOUT_SIZE is -1,then v2 = v1
     *  v3 = Calculate the number of instances based on the available resources of the queue.'
     *  v4 = Calculate the number of instances based on required cores
     *  rule --> spark.executor.instances = max(v1, min(max(v2,v4), v3))
     */
    @Test
    public void testExecutorInstancesRule() {
        ExecutorInstancesRule instancesRule = new ExecutorInstancesRule();
        SparkConfHelper helper = new SparkConfHelper();
        helper.setClusterManager(clusterManager);
        // case: v1 == v2 > v3, spark.executor.instances = v1
        resetSparkConfHelper(helper);
        Mockito.when(clusterManager.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(100, 100), new ResourceInfo(60480, 100)));
        instancesRule.apply(helper);
        Assert.assertEquals("5", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: v1 == v2 < v4 < v3, spark.executor.instances = v4
        resetSparkConfHelper(helper);
        Mockito.when(clusterManager.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(20480, 100), new ResourceInfo(60480, 100)));
        instancesRule.apply(helper);
        Assert.assertEquals("13", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: v1 < v4 <  v2 < v3, spark.executor.instances = v2
        resetSparkConfHelper(helper);
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "200");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "9");
        instancesRule.apply(helper);
        Assert.assertEquals("10", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: v1 < v2 < v4 < v3, spark.executor.instances = v4
        resetSparkConfHelper(helper);
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "200");
        instancesRule.apply(helper);
        Assert.assertEquals("13", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: v2 < v4 < v3 < v1, spark.executor.instances = v1
        resetSparkConfHelper(helper);
        overwriteSystemProp("kylin.engine.base-executor-instance", "30");
        instancesRule.apply(helper);
        Assert.assertEquals("30", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: v1 <  v 4 < v2 < v3, spark.executor.instances = v2
        restoreSystemProp("kylin.engine.base-executor-instance");
        resetSparkConfHelper(helper);
        Mockito.when(clusterManager.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(60480, 100), new ResourceInfo(60480, 100)));
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "1200");
        instancesRule.apply(helper);
        Assert.assertEquals("20", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: SparkConfHelper.LAYOUT_SIZE=-1, v4 < v1 = v2 < v3 , spark.executor.instances = v1
        resetSparkConfHelper(helper);
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "-1");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "4");
        instancesRule.apply(helper);
        Assert.assertEquals("5", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: user defined executor instances
        resetSparkConfHelper(helper);
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, "10");
        instancesRule.apply(helper);
        Assert.assertEquals("10", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

    }

    private void resetSparkConfHelper(SparkConfHelper helper) {
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, "1b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "1");
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "default");
        helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, "1GB");
        helper.setConf(SparkConfHelper.EXECUTOR_INSTANCES, null);
        helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, "512MB");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "14");
        helper.setOption(SparkConfHelper.EXECUTOR_INSTANCES, null);
    }

    private void cleanSparkConfHelper(SparkConfHelper helper) {
        helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, null);
        helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, null);
        helper.setConf(SparkConfHelper.EXECUTOR_CORES, null);
    }

    @Test
    public void testExecutorInstancesRuleWhenError() {
        SparkConfHelper helper = new SparkConfHelper();
        Mockito.when(clusterManager.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(-60480, -100), new ResourceInfo(-60480, -100)));
        helper.generateSparkConf();
        Assert.assertEquals("5", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));
    }

    @Test
    public void testYarnConfRule() throws JsonProcessingException {
        // auto set default yarn queue
        SparkConfHelper helper = new SparkConfHelper();
        helper.setClusterManager(clusterManager);
        YarnConfRule yarnConfRule = new YarnConfRule();
        yarnConfRule.apply(helper);
        Assert.assertEquals("default", helper.getConf(SparkConfHelper.DEFAULT_QUEUE));
        SparkConf sparkConf = new SparkConf();
        helper.applySparkConf(sparkConf);
        Assert.assertEquals("default", sparkConf.get(SparkConfHelper.DEFAULT_QUEUE));

        // set exist yarn queue
        List<String> queueNames = Lists.newArrayList("default", "dw");
        Mockito.when(clusterManager.listQueueNames()).thenReturn(queueNames);
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "dw");
        yarnConfRule.apply(helper);
        Assert.assertEquals("dw", helper.getConf(SparkConfHelper.DEFAULT_QUEUE));
        helper.applySparkConf(sparkConf);
        Assert.assertEquals("dw", sparkConf.get(SparkConfHelper.DEFAULT_QUEUE));

        // set non-exist yarn queue
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "test");
        yarnConfRule.apply(helper);
        Assert.assertEquals("default", helper.getConf(SparkConfHelper.DEFAULT_QUEUE));
        helper.applySparkConf(sparkConf);
        Assert.assertEquals("default", sparkConf.get(SparkConfHelper.DEFAULT_QUEUE));
    }

    @Test
    public void testAutoSetSparkExecutorMemory() throws JsonProcessingException {
        SparkConfHelper helper = new SparkConfHelper();
        // mock yarn max ApplicationMaster memory and cores
        Mockito.when(clusterManager.fetchMaximumResourceAllocation()).thenReturn(new ResourceInfo(1024, 100));
        helper.setClusterManager(clusterManager);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, "1073741824000b");
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "default");
        SparkConf sparkConf = new SparkConf();
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        // 1024 * 0.9 = 921.6 - 1 = 920.6 ~ 920 MB
        Assert.assertEquals("920MB", sparkConf.get(SparkConfHelper.EXECUTOR_MEMORY));
    }

    private static class CompareTuple {
        String expect_Value;
        String key;

        CompareTuple(String expect_Value, String key) {
            this.expect_Value = expect_Value;
            this.key = key;
        }
    }
}

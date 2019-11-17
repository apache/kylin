/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.utils;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.conf.rule.ExecutorInstancesRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.kyligence.kap.cluster.AvailableResource;
import io.kyligence.kap.cluster.ResourceInfo;
import io.kyligence.kap.cluster.YarnInfoFetcher;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.job.KylinBuildEnv;

public class SparkConfHelperTest extends NLocalFileMetadataTestCase {
    private YarnInfoFetcher fetcher = mock(YarnInfoFetcher.class);

    @Before
    public void setup() throws Exception {
        staticCreateTestMetadata();
        KylinBuildEnv.getOrCreate(KylinConfig.getInstanceFromEnv());
        Mockito.when(fetcher.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(100, 100), new ResourceInfo(60480, 100)));
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    @Test
    public void testOneGradeWhenLessTHan1GB() {
        SparkConfHelper helper = new SparkConfHelper();
        helper.setFetcher(fetcher);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, "1b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
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
        helper.setConf(SparkConfHelper.COUNT_DISTICT, "true");
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.set(0, new CompareTuple("4GB", SparkConfHelper.EXECUTOR_MEMORY));
        compareTuples.set(1, new CompareTuple("5", SparkConfHelper.EXECUTOR_CORES));
        compareTuples.set(2, new CompareTuple("1GB", SparkConfHelper.EXECUTOR_OVERHEAD));
        compareConf(compareTuples, sparkConf);

    }

    @Test
    public void testTwoGradeWhenLessTHan10GB() {
        SparkConfHelper helper = new SparkConfHelper();
        helper.setFetcher(fetcher);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 8L * 1024 * 1024 * 1024 + "b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
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
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.set(0, new CompareTuple("10GB", SparkConfHelper.EXECUTOR_MEMORY));
        compareTuples.set(2, new CompareTuple("2GB", SparkConfHelper.EXECUTOR_OVERHEAD));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testThreeGradeWhenLessHan100GB() {
        SparkConfHelper helper = new SparkConfHelper();
        helper.setFetcher(fetcher);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 50L * 1024 * 1024 * 1024 + "b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
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
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.set(0, new CompareTuple("16GB", SparkConfHelper.EXECUTOR_MEMORY));
        compareTuples.set(2, new CompareTuple("4GB", SparkConfHelper.EXECUTOR_OVERHEAD));
        compareConf(compareTuples, sparkConf);
    }

    @Test
    public void testFourGradeWhenGreaterThanOrEqual100GB() {
        SparkConfHelper helper = new SparkConfHelper();
        helper.setFetcher(fetcher);
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, 100L * 1024 * 1024 * 1024 + "b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
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
        helper.generateSparkConf();
        helper.applySparkConf(sparkConf);
        compareTuples.set(0, new CompareTuple("20GB", SparkConfHelper.EXECUTOR_MEMORY));
        compareTuples.set(2, new CompareTuple("6GB", SparkConfHelper.EXECUTOR_OVERHEAD));
        compareConf(compareTuples, sparkConf);
    }

    private void compareConf(List<CompareTuple> tuples, SparkConf conf) {
        for (CompareTuple tuple : tuples) {
            Assert.assertEquals(tuple.expect_Value, conf.get(tuple.key));
        }
    }

    private class CompareTuple {
        String expect_Value;
        String key;

        CompareTuple(String expect_Value, String key) {
            this.expect_Value = expect_Value;
            this.key = key;
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
        helper.setFetcher(fetcher);
        // case: v1 == v2 > v3, spark.executor.instances = v1
        resetSparkConfHelper(helper);
        Mockito.when(fetcher.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(100, 100), new ResourceInfo(60480, 100)));
        instancesRule.apply(helper);
        Assert.assertEquals("5", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: v1 == v2 < v4 < v3, spark.executor.instances = v4
        resetSparkConfHelper(helper);
        Mockito.when(fetcher.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(20480, 100), new ResourceInfo(60480, 100)));
        instancesRule.apply(helper);
        Assert.assertEquals("14", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

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
        Assert.assertEquals("14", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: v2 < v4 < v3 < v1, spark.executor.instances = v1
        resetSparkConfHelper(helper);
        System.setProperty("kylin.engine.base-executor-instance", "30");
        instancesRule.apply(helper);
        Assert.assertEquals("30", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));

        // case: v1 <  v 4 < v2 < v3, spark.executor.instances = v2
        resetSparkConfHelper(helper);
        Mockito.when(fetcher.fetchQueueAvailableResource("default"))
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

    }

    private void resetSparkConfHelper(SparkConfHelper helper) {
        helper.setOption(SparkConfHelper.SOURCE_TABLE_SIZE, "1b");
        helper.setOption(SparkConfHelper.LAYOUT_SIZE, "10");
        helper.setConf(SparkConfHelper.DEFAULT_QUEUE, "default");
        helper.setConf(SparkConfHelper.EXECUTOR_MEMORY, "1GB");
        helper.setConf(SparkConfHelper.EXECUTOR_OVERHEAD, "512MB");
        helper.setOption(SparkConfHelper.REQUIRED_CORES, "14");

        System.clearProperty("kylin.engine.base-executor-instance");
    }

    @Test
    public void testExecutorInstancesRuleWhenError() {
        SparkConfHelper helper = new SparkConfHelper();
        Mockito.when(fetcher.fetchQueueAvailableResource("default"))
                .thenReturn(new AvailableResource(new ResourceInfo(-60480, -100), new ResourceInfo(-60480, -100)));
        helper.generateSparkConf();
        Assert.assertEquals("5", helper.getConf(SparkConfHelper.EXECUTOR_INSTANCES));
    }

}

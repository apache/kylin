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

package org.apache.spark.sql;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparderContextFacadeTest extends LocalWithSparkSessionTest {

    private static final Logger logger = LoggerFactory.getLogger(SparderContextFacadeTest.class);
    private static final Integer TEST_SIZE = 16 * 1024 * 1024;

    @Override
    @Before
    public void setup() throws SchedulerException {
        super.setup();
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        // the default value of kylin.query.spark-conf.spark.master is yarn,
        // which will read from kylin-defaults.properties
        conf.setProperty("kylin.query.spark-conf.spark.master", "local");
        // Init Sparder
        SparderContext.getOriginalSparkSession();
    }

    @After
    public void after() {
        SparderContext.stopSpark();
        KylinConfig.getInstanceFromEnv()
                .setProperty("kylin.query.spark-conf.spark.master", "yarn");
        super.after();
    }

    @Test
    public void testThreadSparkSession() throws InterruptedException, ExecutionException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 5, 1,
                TimeUnit.DAYS, new LinkedBlockingQueue<>(5));

        // test the thread local SparkSession
        CompletionService<Throwable> service = runThreadSparkSessionTest(executor, false);

        for (int i = 1; i <= 5; i++) {
            Assert.assertNull(service.take().get());
        }

        // test the original SparkSession, it must throw errors.
        service = runThreadSparkSessionTest(executor, true);
        boolean hasError = false;
        for (int i = 1; i <= 5; i++) {
            if (service.take().get() != null) {
                hasError = true;
            }
        }
        Assert.assertTrue(hasError);

        executor.shutdown();
    }

    protected CompletionService<Throwable> runThreadSparkSessionTest(ThreadPoolExecutor executor,
                                                        boolean isOriginal) {
        List<TestCallable> tasks = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            tasks.add(new TestCallable(String.valueOf(TEST_SIZE * i), String.valueOf(i), isOriginal));
        }

        CompletionService<Throwable> service = new ExecutorCompletionService<>(executor);
        for (TestCallable task : tasks) {
            service.submit(task);
        }
        return service;
    }

    class TestCallable implements Callable<Throwable> {

        private String maxPartitionBytes = null;
        private String shufflePartitions = null;
        private boolean isOriginal = false;

        TestCallable(String maxPartitionBytes, String shufflePartitions, boolean isOriginal) {
            this.maxPartitionBytes = maxPartitionBytes;
            this.shufflePartitions = shufflePartitions;
            this.isOriginal = isOriginal;
        }

        @Override
        public Throwable call() throws Exception {
            try {
                SparkSession ss = null;
                if (!this.isOriginal) {
                    ss = SparderContext.getSparkSession();
                } else {
                    ss = SparderContext.getOriginalSparkSession();
                }
                ss.conf().set("spark.sql.files.maxPartitionBytes", this.maxPartitionBytes);
                ss.conf().set("spark.sql.shuffle.partitions", this.shufflePartitions);

                Thread.sleep((new Random()).nextInt(2) * 1000L);
                Assert.assertEquals(this.maxPartitionBytes,
                        ss.conf().get("spark.sql.files.maxPartitionBytes"));
                Assert.assertEquals(this.shufflePartitions,
                        ss.conf().get("spark.sql.shuffle.partitions"));
            } catch (Throwable th) {
                logger.error("Test thread local SparkSession error: ", th);
                return th;
            }
            logger.info("Test thread local SparkSession successfully: {}");
            return null;
        }
    }
}

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

package org.apache.kylin.job;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.lock.ZookeeperJobLock;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.hadoop.cube.StorageCleanupJob;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.invertedindex.IIJob;
import org.apache.kylin.job.invertedindex.IIJobBuilder;
import org.apache.kylin.job.manager.ExecutableManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * 
 * @author shaoshi
 *
 */
public class BuildIIWithEngineTest {

    private JobEngineConfig jobEngineConfig;
    private IIManager iiManager;

    private DefaultScheduler scheduler;
    protected ExecutableManager jobService;

    protected static final String TEST_II_NAME = "test_kylin_ii";
    private static final Log logger = LogFactory.getLog(BuildCubeWithEngineTest.class);

    protected void waitForJob(String jobId) {
        while (true) {
            AbstractExecutable job = jobService.getJob(jobId);
            if (job.getStatus() == ExecutableState.SUCCEED || job.getStatus() == ExecutableState.ERROR) {
                break;
            } else {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty("hdp.version", "2.2.4.2-2"); // mapred-site.xml ref this
    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);

        DeployUtil.initCliWorkDir();
        //        DeployUtil.deployMetadata();
        DeployUtil.overrideJobJarLocations();

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        jobService = ExecutableManager.getInstance(kylinConfig);
        scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(kylinConfig), new ZookeeperJobLock());
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        iiManager = IIManager.getInstance(kylinConfig);
        jobEngineConfig = new JobEngineConfig(kylinConfig);
        for (String jobId : jobService.getAllJobIds()) {
            if (jobService.getJob(jobId) instanceof IIJob) {
                jobService.deleteJob(jobId);
            }
        }

        IIInstance ii = iiManager.getII(TEST_II_NAME);
        if (ii.getStatus() != RealizationStatusEnum.DISABLED) {
            ii.setStatus(RealizationStatusEnum.DISABLED);
            iiManager.updateII(ii);
        }
    }

    @After
    public void after() throws Exception {
        IIInstance ii = iiManager.getII(TEST_II_NAME);
        if (ii.getStatus() != RealizationStatusEnum.READY) {
            ii.setStatus(RealizationStatusEnum.READY);
            iiManager.updateII(ii);
        }
        backup();
    }

    @Test
    public void testBuildII() throws Exception {

        String[] testCase = new String[] { "buildII" };
        ExecutorService executorService = Executors.newFixedThreadPool(testCase.length);
        final CountDownLatch countDownLatch = new CountDownLatch(testCase.length);
        List<Future<List<String>>> tasks = Lists.newArrayListWithExpectedSize(testCase.length);
        for (int i = 0; i < testCase.length; i++) {
            tasks.add(executorService.submit(new TestCallable(testCase[i], countDownLatch)));
        }
        countDownLatch.await();
        for (int i = 0; i < tasks.size(); ++i) {
            Future<List<String>> task = tasks.get(i);
            final List<String> jobIds = task.get();
            for (String jobId : jobIds) {
                assertJobSucceed(jobId);
            }
        }

    }

    private void assertJobSucceed(String jobId) {
        assertEquals(ExecutableState.SUCCEED, jobService.getOutput(jobId).getState());
    }

    private class TestCallable implements Callable<List<String>> {

        private final String methodName;
        private final CountDownLatch countDownLatch;

        public TestCallable(String methodName, CountDownLatch countDownLatch) {
            this.methodName = methodName;
            this.countDownLatch = countDownLatch;
        }

        @SuppressWarnings("unchecked")
        @Override
        public List<String> call() throws Exception {
            try {
                final Method method = BuildIIWithEngineTest.class.getDeclaredMethod(methodName);
                method.setAccessible(true);
                return (List<String>) method.invoke(BuildIIWithEngineTest.this);
            } finally {
                countDownLatch.countDown();
            }
        }
    }

    protected List<String> buildII() throws Exception {
        clearSegment(TEST_II_NAME);

        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        // this cube's start date is 0, end date is 2015-1-1
        long date1 = 0;
        long date2 = f.parse("2015-01-01").getTime();

        // this cube doesn't support incremental build, always do full build

        List<String> result = Lists.newArrayList();
        result.add(buildSegment(TEST_II_NAME, date1, date2));
        return result;
    }

    private void clearSegment(String iiName) throws Exception {
        IIInstance ii = iiManager.getII(iiName);
        ii.getSegments().clear();
        iiManager.updateII(ii);
    }

    private String buildSegment(String iiName, long startDate, long endDate) throws Exception {
        IIInstance iiInstance = iiManager.getII(iiName);
        IISegment segment = iiManager.buildSegment(iiInstance, startDate, endDate);
        iiInstance.getSegments().add(segment);
        iiManager.updateII(iiInstance);
        IIJobBuilder iiJobBuilder = new IIJobBuilder(jobEngineConfig);
        IIJob job = iiJobBuilder.buildJob(segment);
        jobService.addJob(job);
        waitForJob(job.getId());
        return job.getId();
    }

    private int cleanupOldStorage() throws Exception {
        String[] args = { "--delete", "true" };

        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        return exitCode;
    }

    private void backup() throws Exception {
        int exitCode = cleanupOldStorage();
        if (exitCode == 0) {
            exportHBaseData();
        }
    }

    private void exportHBaseData() throws IOException {
        ExportHBaseData export = new ExportHBaseData();
        export.exportTables();
    }

    public static void main(String[] args) throws Exception {
        BuildIIWithEngineTest instance = new BuildIIWithEngineTest();

        BuildIIWithEngineTest.beforeClass();
        instance.before();
        instance.testBuildII();
        instance.after();

    }

}

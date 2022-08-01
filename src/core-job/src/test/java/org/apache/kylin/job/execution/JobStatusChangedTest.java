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

package org.apache.kylin.job.execution;

import static org.awaitility.Awaitility.with;

import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.Epoch;
import org.apache.kylin.common.persistence.metadata.EpochStore;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.awaitility.core.ConditionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class JobStatusChangedTest extends NLocalFileMetadataTestCase {
    String project = "default";
    KylinConfig config;

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        config = KylinConfig.getInstanceFromEnv();
        NProjectManager prjMgr = NProjectManager.getInstance(config);
        prjMgr.createProject(project, "", "", Maps.newLinkedHashMap());
    }

    @Test
    public void test_KE24110_FailSamplingJobWithEpochChanged() throws Exception {
        EpochManager epcMgr = EpochManager.getInstance();
        epcMgr.tryUpdateEpoch(project, true);

        NExecutableManager execMgr = NExecutableManager.getInstance(config, project);
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setJobType(JobTypeEnum.TABLE_SAMPLING);
        job.setProject("default");

        BaseTestExecutable task1 = new SucceedTestExecutable();
        task1.setProject("default");
        job.addTask(task1);

        BaseTestExecutable task2 = new FiveSecondErrorTestExecutable();
        task2.setProject("default");

        job.addTask(task2);
        execMgr.addJob(job);

        config.setProperty("kylin.env", "dev");
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(project);
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
        // record old avail mem
        final double before = NDefaultScheduler.currentAvailableMem();

        // wait until to_failed step2 is running
        ConditionFactory conditionFactory = with().pollInterval(10, TimeUnit.MILLISECONDS) //
                .and().with().pollDelay(10, TimeUnit.MILLISECONDS) //
                .await().atMost(60000, TimeUnit.MILLISECONDS);
        conditionFactory.until(() -> ExecutableState.RUNNING == job.getTasks().get(1).getStatus());

        // after to_failed step2 is running, change epoch
        Epoch epoch = epcMgr.getEpoch(project);
        epoch.setEpochId(epoch.getEpochId() + 1);
        EpochStore epochStore = EpochStore.getEpochStore(config);
        epochStore.update(epoch);
        // wait util job finished
        conditionFactory.until(() -> before == NDefaultScheduler.currentAvailableMem());

        // to_failed step2 can not update job status due to epoch changed
        Assert.assertEquals(ExecutableState.RUNNING, job.getStatus());
    }
}

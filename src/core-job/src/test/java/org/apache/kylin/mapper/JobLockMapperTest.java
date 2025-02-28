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

package org.apache.kylin.mapper;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.mapper.JobLockMapper;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

@MetadataInfo(onlyProps = true)
public class JobLockMapperTest {
    private JobLockMapper jobLockMapper;

    @BeforeEach
    public void setup() throws Exception {
        KylinConfig config = getTestConfig();
        JobContextUtil.cleanUp();
        ReflectionTestUtils.invokeMethod(JobContextUtil.class, "initMappers", config);
        config.setProperty("kylin.job.slave-lock-renew-sec", "3");
        jobLockMapper = (JobLockMapper) ReflectionTestUtils.getField(JobContextUtil.class, "jobLockMapper");
    }

    @AfterEach
    public void clean() {
        JobContextUtil.cleanUp();
    }

    private JobLock generateJobLock() {

        JobLock jobLock = new JobLock();
        jobLock.setProject("default");
        jobLock.setLockId("mock_lock_id");
        jobLock.setLockNode("mock_lock_node");
        long renewalSec = getTestConfig().getJobSchedulerJobRenewalSec();
        jobLock.setLockExpireTime(new Date(System.currentTimeMillis() + renewalSec * 1000));

        return jobLock;
    }

    @Test
    public void jobLockCrud() {
        // create
        JobLock jobLock = generateJobLock();
        int insertAffect = jobLockMapper.insert(jobLock);
        Assert.assertEquals(1, insertAffect);

        // read
        String jobLockNode = jobLockMapper.findNodeByLockId("mock_lock_id");
        Assert.assertEquals("mock_lock_node", jobLockNode);

        int count = jobLockMapper.getActiveJobLockCount();
        Assert.assertEquals(1, count);

        long renewSec = getTestConfig().getJobSchedulerJobRenewalSec();
        Awaitility.await().atMost(renewSec + 1, TimeUnit.SECONDS)
                .until(() -> jobLockMapper.findNonLockIdList(10, Collections.singletonList("default")).size() > 0);

        // update (h2 no support mysql-dialect)
        //        int updateAffect = jobLockMapper.upsertLock("mock_job_id", "mock_node_id", 10000L);
        //        Assert.assertEquals(1, updateAffect);

        // delete
        int deleteAffect = jobLockMapper.removeLock("mock_lock_id", "mock_lock_node");
        Assert.assertEquals(1, deleteAffect);

        insertAffect = jobLockMapper.insert(jobLock);
        Assert.assertEquals(1, insertAffect);
        deleteAffect = jobLockMapper.deleteAllJobLock();
        Assert.assertEquals(1, deleteAffect);

    }

    @Test
    public void batchRemoveLocksTest() {
        JobLock jobLock = generateJobLock();
        JobLock jobLock2 = generateJobLock();
        jobLock2.setLockId(jobLock2.getLockId() + "_1");

        jobLockMapper.insert(jobLock);
        jobLockMapper.insert(jobLock2);

        int deleteAffect = jobLockMapper.batchRemoveLock(Arrays.asList(jobLock.getLockId(), jobLock2.getLockId()));
        Assert.assertEquals(2, deleteAffect);
    }
}

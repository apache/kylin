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

import java.io.File;
import java.nio.charset.Charset;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DistributedScheduler;
import org.apache.kylin.storage.hbase.util.ZookeeperDistributedJobLock;
import org.apache.kylin.storage.hbase.util.ZookeeperUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class BaseTestDistributedScheduler extends HBaseMetadataTestCase {
    static ExecutableManager execMgr;
    static ZookeeperDistributedJobLock jobLock;
    static DistributedScheduler scheduler1;
    static DistributedScheduler scheduler2;
    static KylinConfig kylinConfig1;
    static KylinConfig kylinConfig2;
    static CuratorFramework zkClient;
    static File localMetaDir;

    static final String SEGMENT_ID = "segmentId";
    static final String segmentId1 = "seg1" + UUID.randomUUID();
    static final String segmentId2 = "seg2" + UUID.randomUUID();
    static final String serverName1 = "serverName1";
    static final String serverName2 = "serverName2";
    static final String confDstPath1 = "target/kylin_metadata_dist_lock_test1/kylin.properties";
    static final String confDstPath2 = "target/kylin_metadata_dist_lock_test2/kylin.properties";

    private static final Logger logger = LoggerFactory.getLogger(BaseTestDistributedScheduler.class);

    @BeforeClass
    public static void setup() throws Exception {
        staticCreateTestMetadata();
        System.setProperty("kylin.job.lock", "org.apache.kylin.storage.hbase.util.ZookeeperDistributedJobLock");

        new File(confDstPath1).getParentFile().mkdirs();
        new File(confDstPath2).getParentFile().mkdirs();
        KylinConfig srcConfig = KylinConfig.getInstanceFromEnv();

        localMetaDir = Files.createTempDir();
        String backup = srcConfig.getMetadataUrl();
        srcConfig.setProperty("kylin.metadata.url", localMetaDir.getAbsolutePath());
        srcConfig.writeProperties(new File(confDstPath1));
        srcConfig.writeProperties(new File(confDstPath2));
        srcConfig.setProperty("kylin.metadata.url", backup);

        kylinConfig1 = KylinConfig.createInstanceFromUri(new File(confDstPath1).getAbsolutePath());
        kylinConfig2 = KylinConfig.createInstanceFromUri(new File(confDstPath2).getAbsolutePath());

        initZk();

        if (jobLock == null)
            jobLock = new ZookeeperDistributedJobLock(kylinConfig1);

        execMgr = ExecutableManager.getInstance(kylinConfig1);
        for (String jobId : execMgr.getAllJobIds()) {
            execMgr.deleteJob(jobId);
        }

        scheduler1 = DistributedScheduler.getInstance(kylinConfig1);
        scheduler1.setServerName(serverName1);
        scheduler1.init(new JobEngineConfig(kylinConfig1), jobLock);
        if (!scheduler1.hasStarted()) {
            throw new RuntimeException("scheduler1 not started");
        }

        scheduler2 = DistributedScheduler.getInstance(kylinConfig2);
        scheduler2.setServerName(serverName2);
        scheduler2.init(new JobEngineConfig(kylinConfig2), jobLock);
        if (!scheduler2.hasStarted()) {
            throw new RuntimeException("scheduler2 not started");
        }

        Thread.sleep(10000);
    }

    @AfterClass
    public static void after() throws Exception {
        if (scheduler1 != null) {
            scheduler1.shutdown();
            scheduler1 = null;
        }
        if (scheduler2 != null) {
            scheduler2.shutdown();
            scheduler2 = null;
        }
        if (jobLock != null) {
            jobLock.close();
            jobLock = null;
        }
        if (zkClient != null) {
            zkClient.close();
            zkClient = null;
        }

        FileUtils.deleteDirectory(localMetaDir);
        System.clearProperty("kylin.job.lock");
        staticCleanupTestMetadata();
    }

    void waitForJobFinish(String jobId) {
        while (true) {
            AbstractExecutable job = execMgr.getJob(jobId);
            final ExecutableState status = job.getStatus();
            if (status == ExecutableState.SUCCEED || status == ExecutableState.ERROR || status == ExecutableState.STOPPED || status == ExecutableState.DISCARDED) {
                break;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    void waitForJobStatus(String jobId, ExecutableState state, long interval) {
        while (true) {
            AbstractExecutable job = execMgr.getJob(jobId);
            if (state == job.getStatus()) {
                break;
            } else {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    boolean lock(ZookeeperDistributedJobLock jobLock, String cubeName, String serverName) {
        return jobLock.lockWithName(cubeName, serverName);
    }

    private static void initZk() {
        String zkConnectString = ZookeeperUtil.getZKConnectString();
        if (StringUtils.isEmpty(zkConnectString)) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM is empty!");
        }
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zkClient = CuratorFrameworkFactory.newClient(zkConnectString, retryPolicy);
        zkClient.start();
    }

    String getServerName(String cubeName) {
        String lockPath = getLockPath(cubeName);
        String serverName = null;
        if (zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
            try {
                if (zkClient.checkExists().forPath(lockPath) != null) {
                    byte[] data = zkClient.getData().forPath(lockPath);
                    serverName = new String(data, Charset.forName("UTF-8"));
                }
            } catch (Exception e) {
                logger.error("get the serverName failed", e);
            }
        }
        return serverName;
    }

    private String getLockPath(String pathName) {
        return ZookeeperDistributedJobLock.ZOOKEEPER_LOCK_PATH + "/" + kylinConfig1.getMetadataUrlPrefix() + "/" + pathName;
    }
}

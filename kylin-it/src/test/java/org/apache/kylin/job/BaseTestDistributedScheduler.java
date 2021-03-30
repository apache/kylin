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

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.ZKUtil;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.impl.threadpool.DistributedScheduler;
import org.apache.kylin.job.lock.zookeeper.ZookeeperDistributedLock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.io.Files;

public class BaseTestDistributedScheduler extends HBaseMetadataTestCase {
    static ExecutableManager execMgr;
    static DistributedScheduler scheduler1;
    static DistributedScheduler scheduler2;
    static ZookeeperDistributedLock jobLock1;
    static ZookeeperDistributedLock jobLock2;
    static KylinConfig kylinConfig1;
    static KylinConfig kylinConfig2;
    static CuratorFramework zkClient;
    static File localMetaDir;
    static String backup;

    static final String jobId1 = RandomUtil.randomUUID().toString();
    static final String jobId2 = RandomUtil.randomUUID().toString();
    static final String serverName1 = "serverName1";
    static final String serverName2 = "serverName2";
    static final String confDstPath1 = "target/kylin_metadata_dist_lock_test1/kylin.properties";
    static final String confDstPath2 = "target/kylin_metadata_dist_lock_test2/kylin.properties";

    private static final Logger logger = LoggerFactory.getLogger(BaseTestDistributedScheduler.class);

    @BeforeClass
    public static void setup() throws Exception {
        staticCreateTestMetadata();

        new File(confDstPath1).getParentFile().mkdirs();
        new File(confDstPath2).getParentFile().mkdirs();
        KylinConfig srcConfig = KylinConfig.getInstanceFromEnv();

        localMetaDir = Files.createTempDir();
        backup = srcConfig.getMetadataUrl().toString();
        srcConfig.setProperty("kylin.metadata.url", localMetaDir.getAbsolutePath());
        srcConfig.exportToFile(new File(confDstPath1));
        srcConfig.exportToFile(new File(confDstPath2));

        kylinConfig1 = KylinConfig.createInstanceFromUri(new File(confDstPath1).getAbsolutePath());
        kylinConfig2 = KylinConfig.createInstanceFromUri(new File(confDstPath2).getAbsolutePath());

        initZk();
        
        ZookeeperDistributedLock.Factory factory = new ZookeeperDistributedLock.Factory(kylinConfig1);
        jobLock1 = (ZookeeperDistributedLock) factory.lockForClient(serverName1);
        jobLock2 = (ZookeeperDistributedLock) factory.lockForClient(serverName2);

        execMgr = ExecutableManager.getInstance(kylinConfig1);
        for (String jobId : execMgr.getAllJobIds()) {
            execMgr.deleteJob(jobId);
        }

        scheduler1 = DistributedScheduler.getInstance(kylinConfig1);
        scheduler1.init(new JobEngineConfig(kylinConfig1), jobLock1);
        if (!scheduler1.hasStarted()) {
            throw new RuntimeException("scheduler1 not started");
        }

        scheduler2 = DistributedScheduler.getInstance(kylinConfig2);
        scheduler2.init(new JobEngineConfig(kylinConfig2), jobLock2);
        if (!scheduler2.hasStarted()) {
            throw new RuntimeException("scheduler2 not started");
        }

        Thread.sleep(10000);
    }

    @AfterClass
    public static void after() throws Exception {
        jobLock1.purgeLocks(DistributedScheduler.ZOOKEEPER_LOCK_PATH);
        
        if (scheduler1 != null) {
            scheduler1.shutdown();
            scheduler1 = null;
        }
        if (scheduler2 != null) {
            scheduler2.shutdown();
            scheduler2 = null;
        }
        if (zkClient != null) {
            zkClient.close();
            zkClient = null;
        }

        FileUtils.deleteDirectory(localMetaDir);
        System.clearProperty("kylin.metadata.url");
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

    boolean lock(ZookeeperDistributedLock jobLock, String segName) {
        return jobLock.lock(DistributedScheduler.getLockPath(segName));
    }

    private static void initZk() {
        zkClient = ZKUtil.newZookeeperClient();
    }

    String getServerName(String segName) {
        String lockPath = DistributedScheduler.getLockPath(segName);
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
}

/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.engine;

/** 
 * @author George Song (ysong1), xduo
 * 
 */
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HadoopUtil;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math.stat.descriptive.rank.Percentile;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobConstants;
import com.kylinolap.job.exception.JobException;

public class JobEngine implements ConnectionStateListener {

    private static Logger log = LoggerFactory.getLogger(JobEngine.class);

    private final String engineID;

    private final JobEngineConfig engineConfig;
    private final QuatzScheduler scheduler;
    private InterProcessMutex sharedLock;
    private CuratorFramework zkClient;

    private static final String ZOOKEEPER_LOCK_PATH = "/kylin/job_engine/lock";
    private static final ConcurrentHashMap<JobEngineConfig, JobEngine> CACHE = new ConcurrentHashMap<JobEngineConfig, JobEngine>();
    private int daemonJobIntervalInSeconds;

    public static JobEngine getInstance(String engineID, JobEngineConfig engineCfg) throws JobException {
        JobEngine r = CACHE.get(engineCfg);
        if (r == null) {
            r = new JobEngine(engineID, engineCfg);
            CACHE.putIfAbsent(engineCfg, r);
        }
        return r;
    }

    private String getZKConnectString(JobEngineConfig context)
    {
        Configuration conf =HadoopUtil.newHBaseConfiguration(context.getConfig().getStorageUrl());
        return conf.get(HConstants.ZOOKEEPER_QUORUM);
    }

    private JobEngine(String engineID, JobEngineConfig context) throws JobException {
        log.info("Using metadata url: " + context.getConfig());

        String ZKConnectString = getZKConnectString(context);
        if (StringUtils.isEmpty(ZKConnectString)) {
            throw new IllegalArgumentException("ZOOKEEPER_QUORUM is empty!");
        }

        this.engineID = engineID;
        this.engineConfig = context;
        this.scheduler = new QuatzScheduler();

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        this.zkClient = CuratorFrameworkFactory.newClient(ZKConnectString, retryPolicy);
        this.zkClient.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.debug("Closing HBASE connection");
                releaseLock();
            }
        });
    }

    private void releaseLock() {
        try {
            if (sharedLock != null && sharedLock.isAcquiredInThisProcess()) {
                sharedLock.release();
            }
            if (zkClient.getState().equals(CuratorFrameworkState.STARTED)) {
                // client.setData().forPath(ZOOKEEPER_LOCK_PATH, null);
                if (zkClient.checkExists().forPath(ZOOKEEPER_LOCK_PATH + "/" + this.engineID) != null) {
                    zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZOOKEEPER_LOCK_PATH + "/" + this.engineID);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void start(int daemonJobIntervalInSeconds) throws Exception {
        this.daemonJobIntervalInSeconds = daemonJobIntervalInSeconds;

        sharedLock = new InterProcessMutex(zkClient, ZOOKEEPER_LOCK_PATH + "/" + this.engineID);
        log.info("Trying to obtain the shared lock...");
        // current thread will be blocked until the lock is got
        sharedLock.acquire();

        log.info("Obtained the shared lock. Starting job scheduler...");
        zkClient.setData().forPath(ZOOKEEPER_LOCK_PATH + "/" + this.engineID, Bytes.toBytes(this.engineID));
        startScheduler();
    }

    private void startScheduler() throws JobException, IOException {
        String logDir = KylinConfig.getInstanceFromEnv().getKylinJobLogDir();
        new File(logDir).mkdirs();

        log.info("Starting scheduler.");
        this.scheduler.start();
        this.scheduler.scheduleFetcher(this.daemonJobIntervalInSeconds, this.engineConfig);
    }

    public void start() throws Exception {
        start(JobConstants.DEFAULT_SCHEDULER_INTERVAL_SECONDS);
    }

    public void stop() throws JobException {
        releaseLock();
        this.scheduler.stop();
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if ((newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST)) {
            releaseLock();
        }
    }

    public void interruptJob(JobInstance jobInstance, JobStep jobStep) throws IOException, JobException {
        // kill the running step
        this.scheduler.interrupt(jobInstance, jobStep);
    }

    public Scheduler getScheduler() {
        return this.scheduler.getScheduler();
    }

    // Job engine metrics related methods

    // <StepID, Duration Seconds>
    public static ConcurrentHashMap<String, Double> JOB_DURATION = new ConcurrentHashMap<String, Double>();

    public int getNumberOfJobStepsExecuted() {
        return JOB_DURATION.values().size();
    }

    public String getPrimaryEngineID() throws Exception {
        byte[] data = zkClient.getData().forPath(ZOOKEEPER_LOCK_PATH + "/" + this.engineID);
        if (data == null) {
            return "";
        } else {
            return Bytes.toString(data);
        }
    }

    public double getMinJobStepDuration() {
        double[] all = getJobStepDuration();
        Arrays.sort(all);

        if (all.length > 0) {
            return all[0];
        } else {
            return 0;
        }
    }

    private double[] getJobStepDuration() {
        Collection<Double> values = JOB_DURATION.values();
        Double[] all = (Double[]) values.toArray(new Double[values.size()]);
        return ArrayUtils.toPrimitive(all);
    }

    public double getMaxJobStepDuration() {
        double[] all = getJobStepDuration();
        Arrays.sort(all);

        if (all.length > 1) {
            return all[all.length - 1];
        } else {
            return 0;
        }
    }

    public double getPercentileJobStepDuration(double percentile) {
        Collection<Double> values = JOB_DURATION.values();
        Double[] all = (Double[]) values.toArray(new Double[values.size()]);
        Percentile p = new Percentile(percentile);
        return p.evaluate(ArrayUtils.toPrimitive(all));
    }

    public Integer getScheduledJobsSzie() {
        return scheduler.getScheduledJobs();
    }

    public int getEngineThreadPoolSize() {
        return scheduler.getThreadPoolSize();
    }

    public int getNumberOfIdleSlots() {
        return scheduler.getIdleSlots();
    }

    public int getNumberOfJobStepsRunning() {
        return scheduler.getRunningJobs();
    }

}

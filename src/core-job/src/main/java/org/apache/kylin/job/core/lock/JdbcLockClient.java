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

package org.apache.kylin.job.core.lock;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.util.ThreadUtils;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcLockClient {

    private static final Logger logger = LoggerFactory.getLogger(JdbcLockClient.class);

    private final JobContext jobContext;

    private ScheduledExecutorService scheduler;

    private Map<String, JdbcJobLock> renewalMap;

    public JdbcLockClient(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    public void start() {
        scheduler = ThreadUtils.newDaemonThreadScheduledExecutor(
                jobContext.getKylinConfig().getJobLockClientRenewalMaxThreads(), "JdbcJobLockClient");

        renewalMap = Maps.newConcurrentMap();
    }

    public void destroy() {
        if (Objects.nonNull(scheduler)) {
            scheduler.shutdownNow();
        }

        if (Objects.nonNull(renewalMap)) {
            renewalMap = null;
        }
    }

    public void stopRenew(String lockId) {
        if (Objects.nonNull(renewalMap)) {
            renewalMap.remove(lockId);
        }
    }

    public boolean tryAcquire(JdbcJobLock jobLock) throws LockException {
        boolean acquired = tryAcquireInternal(jobLock);
        if (acquired) {
            // register renewal
            renewalMap.put(jobLock.getLockId(), jobLock);
            // auto renewal
            scheduler.schedule(() -> renewal(jobLock), jobLock.getRenewalDelaySec(), TimeUnit.SECONDS);
        }
        return acquired;
    }

    public boolean tryRelease(JdbcJobLock jobLock) throws LockException {
        try {
            int r = jobContext.getJobLockMapper().removeLock(jobLock.getLockId(), jobLock.getLockNode());
            return r > 0;
        } catch (Exception e) {
            throw new LockException("Release lock failed", e);
        } finally {
            stopRenew(jobLock.getLockId());
        }
    }

    private void renewal(JdbcJobLock jobLock) {
        if (!renewalMap.containsKey(jobLock.getLockId())) {
            logger.info("Renewal skip released lock {}", jobLock.getLockId());
            return;
        }
        boolean acquired = false;
        try {
            acquired = tryAcquireInternal(jobLock);
        } catch (LockException e) {
            logger.error("Renewal lock failed", e);
        }
        if (acquired) {
            // schedule next renewal automatically
            scheduler.schedule(() -> renewal(jobLock), jobLock.getRenewalDelaySec(), TimeUnit.SECONDS);
        }
    }

    private boolean tryAcquireInternal(JdbcJobLock jobLock) throws LockException {
        boolean acquired = false;
        try {
            int r = jobContext.getJobLockMapper().updateLock(jobLock.getLockId(), jobLock.getLockNode(),
                    jobLock.getRenewalSec(), System.currentTimeMillis());
            acquired = r > 0;
            return acquired;
        } catch (Exception e) {
            throw new LockException("Acquire lock failed", e);
        } finally {
            if (acquired) {
                jobLock.getAcquireListener().onSucceed();
            } else {
                jobLock.getAcquireListener().onFailed();
            }
        }
    }

}

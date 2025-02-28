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

package org.apache.kylin.job.scheduler;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SystemInfoCollector;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.core.AbstractJobExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceAcquirer {

    private static final Logger logger = LoggerFactory.getLogger(ResourceAcquirer.class);

    private final KylinConfig kylinConfig;

    private final ConcurrentMap<String, NodeResource> registers;

    private static volatile Semaphore memorySemaphore = new Semaphore(Integer.MAX_VALUE);

    public ResourceAcquirer(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;

        if (kylinConfig.getAutoSetConcurrentJob()) {
            double memoryRatio = kylinConfig.getMaxLocalConsumptionRatio();
            if (Integer.MAX_VALUE == memorySemaphore.availablePermits()) {
                memorySemaphore = new Semaphore((int) (memoryRatio * SystemInfoCollector.getAvailableMemoryInfo()));
            }
            logger.info("Init memorySemaphore:{} MB, memoryRatio: {}", memorySemaphore.availablePermits(), memoryRatio);
        }
        registers = Maps.newConcurrentMap();
    }

    public boolean tryAcquire(AbstractJobExecutable jobExecutable) {
        if (kylinConfig.getDeployMode().equals("cluster")) {
            logger.debug("Submit job with 'cluster' mode, skip acquire driver resource.");
            return true;
        }
        NodeResource resource = new NodeResource(jobExecutable);
        boolean acquired = memorySemaphore.tryAcquire(resource.getMemory());
        if (acquired) {
            registers.put(jobExecutable.getJobId(), resource);
            logger.info("Acquire resource success {}, available: {}MB", resource, memorySemaphore.availablePermits());
            return true;
        }
        logger.warn("Acquire resource failed {}, available: {}MB", resource, memorySemaphore.availablePermits());
        return false;
    }

    public void release(AbstractJobExecutable jobExecutable) {
        if (kylinConfig.getDeployMode().equals("cluster")) {
            return;
        }
        String jobId = jobExecutable.getJobId();
        NodeResource resource = registers.get(jobId);
        if (Objects.isNull(resource)) {
            logger.warn("Cannot find job's registered resource: {}", jobId);
            return;
        }
        memorySemaphore.release(resource.getMemory());
        registers.remove(jobExecutable.getJobId());
        logger.info("Release resource success {}, available: {}MB", resource, memorySemaphore.availablePermits());
    }

    public static double currentAvailableMem() {
        return 1.0 * memorySemaphore.availablePermits();
    }

    public static int availablePermits() {
        return memorySemaphore.availablePermits();
    }

    public void start() {
        // do nothing
    }

    public void destroy() {
        // do nothing
    }
}

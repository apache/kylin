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
package org.apache.kylin.rest.service;

import org.apache.spark.scheduler.ContainerSchedulerManager;
import org.apache.spark.sql.SparderEnv;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class QueryResourceService {

    public String getQueueName() {
        String queue = getContainerSchedulerManager().getQueueName();
        log.info("queueName={}", queue);
        return queue;
    }

    public QueryResource adjustQueryResource(QueryResource resource) {
        val manager = getContainerSchedulerManager();
        int cores = resource.getCores();
        long memory = resource.getMemory();
        val core = cores / manager.getExecutorCores();
        val mem = memory / manager.getExecutorMemory();
        int adjustNum = (int) Math.min(core, mem);
        if (adjustNum == 0) {
            return new QueryResource();
        }
        adjustNum = updateExecutorNum(adjustNum, resource.force);
        return new QueryResource(resource.queueName, manager.getExecutorCores() * adjustNum,
                manager.getExecutorMemory() * adjustNum, resource.force);
    }

    private int updateExecutorNum(int num, boolean force) {
        val manager = getContainerSchedulerManager();
        if (num < 0) {
            num *= -1;
            val available = manager.getExecutorCount() - 1;
            num = Math.min(num, available);
            return num == 0 ? 0 : manager.releaseExecutor(num, force).size() * -1;
        }
        return manager.requestExecutor(num) ? num : 0;
    }

    private ContainerSchedulerManager getContainerSchedulerManager() {
        return SparderEnv.containerSchedulerManager().get();
    }

    public boolean isAvailable() {
        boolean available = SparderEnv.containerSchedulerManager().isDefined() && SparderEnv.isSparkAvailable();
        log.info("node is available={}", available);
        return available;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class QueryResource {

        private String queueName;
        private int cores;
        private long memory;
        private boolean force;
    }

}

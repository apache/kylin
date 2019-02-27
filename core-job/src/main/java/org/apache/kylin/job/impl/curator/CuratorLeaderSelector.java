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

package org.apache.kylin.job.impl.curator;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorLeaderSelector extends LeaderSelectorListenerAdapter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(CuratorLeaderSelector.class);
    private final String name;
    private final LeaderSelector leaderSelector;
    private JobEngineConfig jobEngineConfig;
    private DefaultScheduler defaultScheduler = null;

    public CuratorLeaderSelector(CuratorFramework client, String path, String name, JobEngineConfig jobEngineConfig) {
        this.name = name;
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
        this.jobEngineConfig = jobEngineConfig;
    }

    public void start() throws IOException {
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
        logger.info(name + " is stopped");
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        logger.info(name + " is the leader for job engine now.");
        defaultScheduler = DefaultScheduler.createInstance();
        defaultScheduler.init(jobEngineConfig, new MockJobLock());

        try {
            while (true) {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5L));
            }
        } catch (InterruptedException var6) {
            logger.error(this.name + " was interrupted.", var6);
            Thread.currentThread().interrupt();
        } finally {
            logger.warn(this.name + " relinquishing leadership.");
            if (defaultScheduler != null)
                defaultScheduler.shutdown();
        }
    }
}
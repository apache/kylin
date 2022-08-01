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

package io.kyligence.kap.clickhouse.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import com.fasterxml.jackson.core.type.TypeReference;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractClickHouseClean extends AbstractExecutable {
    public static final String CLICKHOUSE_SHARD_CLEAN_PARAM = "P_CLICKHOUSE_SHARD_CLEAN";
    public static final String CLICKHOUSE_NODE_COUNT_PARAM = "P_CLICKHOUSE_NODE_COUNT";
    public static final String THREAD_NAME = "CLICKHOUSE_CLEAN";
    protected List<ShardCleaner> shardCleaners = new ArrayList<>();
    private int nodeCount = 10;

    public AbstractClickHouseClean() {
        super();
    }

    public AbstractClickHouseClean(Object notSetId) {
        super(notSetId);
    }

    public void setNodeCount(int nodeCount) {
        if (nodeCount > 0) {
            this.nodeCount = nodeCount;
        }
    }

    protected void saveState() {
        this.setParam(CLICKHOUSE_SHARD_CLEAN_PARAM, JsonUtil.writeValueAsStringQuietly(shardCleaners));
        this.setParam(CLICKHOUSE_NODE_COUNT_PARAM, String.valueOf(nodeCount));
    }

    protected void loadState() {
        try {
            shardCleaners = JsonUtil.readValue(this.getParam(CLICKHOUSE_SHARD_CLEAN_PARAM),
                    new TypeReference<List<ShardCleaner>>() {
                    });
            this.nodeCount = Integer.parseInt(this.getParam(CLICKHOUSE_NODE_COUNT_PARAM));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    public void init() {
        internalInit();
        saveState();
    }

    protected abstract void internalInit();

    protected abstract Runnable getTask(ShardCleaner shardCleaner);

    protected void workImpl() throws ExecutionException, InterruptedException {
        val taskPool = new ThreadPoolExecutor(nodeCount, nodeCount, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new NamedThreadFactory(THREAD_NAME));
        List<Future<?>> results = new ArrayList<>();
        shardCleaners.forEach(shardCleaner -> {
            val result = taskPool.submit(getTask(shardCleaner));
            results.add(result);
        });
        try {
            for (Future<?> result : results) {
                result.get();
            }
        } finally {
            taskPool.shutdownNow();
            closeShardClean();
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        return wrapWithExecuteException(() -> {
            loadState();
            workImpl();
            return ExecuteResult.createSucceed();
        });
    }

    protected void closeShardClean() {
        if (!shardCleaners.isEmpty()) {
            shardCleaners.forEach(shardCleaner -> shardCleaner.getClickHouse().close());
            shardCleaners.clear();
        }
    }
}

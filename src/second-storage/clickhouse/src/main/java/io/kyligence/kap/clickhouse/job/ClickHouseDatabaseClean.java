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

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_NODE_CLEAN;

import java.sql.SQLException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KylinConfig;

import com.clearspring.analytics.util.Preconditions;

import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseDatabaseClean extends AbstractClickHouseClean {

    public ClickHouseDatabaseClean() {
        setName(STEP_SECOND_STORAGE_NODE_CLEAN);
    }

    public ClickHouseDatabaseClean(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected void internalInit() {
        KylinConfig config = getConfig();
        val nodeGroupManager = SecondStorageUtil.nodeGroupManager(config, getProject());
        Preconditions.checkState(nodeGroupManager.isPresent());
        setNodeCount(Math.toIntExact(nodeGroupManager.map(
                manager -> manager.listAll().stream().mapToLong(nodeGroup -> nodeGroup.getNodeNames().size()).sum())
                .orElse(0L)));
        nodeGroupManager.get().listAll().stream().flatMap(nodeGroup -> nodeGroup.getNodeNames().stream())
                .forEach(node -> {
                    ShardCleaner shardCleaner = new ShardCleaner(node, NameUtil.getDatabase(config, getProject()));
                    shardCleaners.add(shardCleaner);
                });
    }

    @Override
    protected Runnable getTask(ShardCleaner shardCleaner) {
        return () -> {
            try {
                shardCleaner.cleanDatabase();
            } catch (SQLException e) {
                log.error("node {} clean database {} failed", shardCleaner.getClickHouse().getShardName(),
                        shardCleaner.getDatabase());
                ExceptionUtils.rethrow(e);
            }
        };
    }
}

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

import static io.kyligence.kap.secondstorage.SecondStorageConstants.STEP_SECOND_STORAGE_REFRESH_SECONDARY_INDEX;
import static io.kyligence.kap.secondstorage.SecondStorageUtil.getTableFlow;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.secondstorage.NameUtil;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.enums.LockTypeEnum;
import io.kyligence.kap.secondstorage.metadata.Manager;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickhouseRefreshSecondaryIndex extends AbstractExecutable {
    public static final String CLICKHOUSE_ADD_SECONDARY_INDEX = "CLICKHOUSE_ADD_SECONDARY_INDEX";
    public static final String CLICKHOUSE_REMOVE_SECONDARY_INDEX = "CLICKHOUSE_REMOVE_SECONDARY_INDEX";
    public static final String CLICKHOUSE_LAYOUT_ID = "CLICKHOUSE_LAYOUT_ID";

    // can't delete because reflect
    public ClickhouseRefreshSecondaryIndex() {
        setName(STEP_SECOND_STORAGE_REFRESH_SECONDARY_INDEX);
    }

    // can't delete because reflect
    public ClickhouseRefreshSecondaryIndex(Object notSetId) {
        super(notSetId);
    }

    @Override
    public ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        String modelId = getTargetSubject();
        Set<Integer> newIndexes = Sets.newHashSet();
        Set<Integer> toBeDeleteIndexed = Sets.newHashSet();
        long layoutId = Long.parseLong(getParam(CLICKHOUSE_LAYOUT_ID));

        try {
            newIndexes.addAll(JsonUtil.readValue(this.getParam(CLICKHOUSE_ADD_SECONDARY_INDEX),
                    new TypeReference<Set<Integer>>() {
                    }));
            toBeDeleteIndexed.addAll(JsonUtil.readValue(this.getParam(CLICKHOUSE_REMOVE_SECONDARY_INDEX),
                    new TypeReference<Set<Integer>>() {
                    }));
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }

        return wrapWithExecuteException(() -> {
            Manager<NodeGroup> nodeGroupManager = SecondStorage.nodeGroupManager(getConfig(), getProject());
            List<NodeGroup> allGroup = nodeGroupManager.listAll();
            for (NodeGroup nodeGroup : allGroup) {
                if (LockTypeEnum.locked(Lists.newArrayList(LockTypeEnum.LOAD.name()), nodeGroup.getLockTypes())) {
                    logger.info("project={} has been locked, skip the step", getProject());
                    return ExecuteResult.createSkip();
                }
            }

            val dataflow = NDataflowManager.getInstance(getConfig(), getProject()).getDataflow(modelId);
            String database = NameUtil.getDatabase(getConfig(), getProject());
            String table = NameUtil.getTable(dataflow, layoutId);
            List<Future<?>> results = Lists.newArrayList();
            List<SecondStorageNode> nodes = SecondStorageUtil.listProjectNodes(getProject());
            val taskPool = new ThreadPoolExecutor(nodes.size(), nodes.size(), 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), new NamedThreadFactory("Refresh Tiered Storage Index"));
            nodes.forEach(node -> results.add(taskPool.submit(() -> new RefreshSecondaryIndex(node.getName(), database,
                    table, newIndexes, toBeDeleteIndexed, dataflow).refresh())));

            try {
                for (Future<?> result : results) {
                    result.get();
                }
            } finally {
                taskPool.shutdownNow();
            }

            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                getTableFlow(project, modelId)
                        .update(tp -> tp.updateSecondaryIndex(layoutId, newIndexes, toBeDeleteIndexed));
                return null;
            }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);

            return ExecuteResult.createSucceed();
        });
    }
}

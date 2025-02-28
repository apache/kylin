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

package org.apache.kylin.metadata.favorite;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;
import org.apache.kylin.metadata.asynctask.MetadataRestoreTask;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncTaskManager {

    public static final String ASYNC_ACCELERATION_TASK = "async_acceleration_task";
    public static final String METADATA_RECOVER_TASK = "metadata_recover_task";
    public static final List<String> ALL_TASK_TYPES = Lists.newArrayList(ASYNC_ACCELERATION_TASK,
            METADATA_RECOVER_TASK);

    private final AsyncTaskStore asyncTaskStore;
    private final String project;

    @SuppressWarnings("unused")
    private AsyncTaskManager(String project) throws Exception {
        this.project = project;
        this.asyncTaskStore = new AsyncTaskStore(KylinConfig.getInstanceFromEnv());
    }

    public static AsyncTaskManager getInstance(String project) {
        return Singletons.getInstance(project, AsyncTaskManager.class);
    }

    public DataSourceTransactionManager getTransactionManager() {
        return asyncTaskStore.getTransactionManager();
    }

    public void save(AbstractAsyncTask asyncTask) {
        if (asyncTask.getTaskType().equalsIgnoreCase(ASYNC_ACCELERATION_TASK)) {
            saveOrUpdateAsyncAccelerationTask(asyncTask);
        } else if (asyncTask.getTaskType().equalsIgnoreCase(METADATA_RECOVER_TASK)) {
            saveOrUpdateMetadataRestoreTask(asyncTask);
        }
    }

    private void saveOrUpdateMetadataRestoreTask(AbstractAsyncTask asyncTask) {
        AbstractAsyncTask asyncTask1 = get(asyncTask.getTaskType(), asyncTask.getTaskKey());
        if (asyncTask1 == null) {
            asyncTask.setCreateTime(System.currentTimeMillis());
            asyncTask.setUpdateTime(asyncTask.getCreateTime());
            asyncTaskStore.save(asyncTask);
        } else {
            asyncTask.setUpdateTime(System.currentTimeMillis());
            asyncTaskStore.update(asyncTask);
        }
    }

    private void saveOrUpdateAsyncAccelerationTask(AbstractAsyncTask asyncTask) {
        asyncTask.setTaskKey(project);
        if (asyncTask.getId() == 0) {
            asyncTask.setProject(project);
            asyncTask.setCreateTime(System.currentTimeMillis());
            asyncTask.setUpdateTime(asyncTask.getCreateTime());
            asyncTaskStore.save(asyncTask);
        } else {
            asyncTask.setUpdateTime(System.currentTimeMillis());
            asyncTaskStore.update(asyncTask);
        }
    }

    public <T extends AbstractAsyncTask> T copyForWrite(T task) {
        // No need to copy, just return the origin object
        // This will be rewrite after metadata is refactored
        return task;
    }

    public AbstractAsyncTask get(String taskType) {
        return get(taskType, project);
    }

    public AbstractAsyncTask get(String taskType, String taskKey) {
        if (!taskType.equalsIgnoreCase(ASYNC_ACCELERATION_TASK) && !taskType.equalsIgnoreCase(METADATA_RECOVER_TASK)) {
            throw new IllegalArgumentException("TaskType " + taskType + "is not supported!");
        }
        AbstractAsyncTask asyncTask = asyncTaskStore.queryByTypeAndKey(taskType, taskKey);
        switch (taskType) {
        case ASYNC_ACCELERATION_TASK:
            if (asyncTask == null) {
                return new AsyncAccelerationTask(false, Maps.newHashMap(), ASYNC_ACCELERATION_TASK);
            } else {
                return AsyncAccelerationTask.copyFromAbstractTask(asyncTask);
            }
        case METADATA_RECOVER_TASK:
            return MetadataRestoreTask.copyFromAbstractTask(asyncTask);
        default:
            return asyncTask;
        }
    }

    public List<AbstractAsyncTask> getAllAsyncTaskByType(String taskType) {
        if (!taskType.equalsIgnoreCase(ASYNC_ACCELERATION_TASK) && !taskType.equalsIgnoreCase(METADATA_RECOVER_TASK)) {
            throw new IllegalArgumentException("TaskType " + taskType + "is not supported!");
        }
        return asyncTaskStore.queryByType(taskType);
    }

    public static void resetAccelerationTagMap(String project) {
        log.info("reset acceleration tag for project({})", project);
        AsyncTaskManager manager = getInstance(project);
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) manager.get(ASYNC_ACCELERATION_TASK);
            AsyncAccelerationTask copied = manager.copyForWrite(asyncAcceleration);
            copied.setAlreadyRunning(false);
            copied.setUserRefreshedTagMap(Maps.newHashMap());
            manager.save(asyncAcceleration);
            return null;
        });
        log.info("rest acceleration tag successfully for project({})", project);
    }

    public static void cleanAccelerationTagByUser(String project, String userName) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        if (!projectInstance.isSemiAutoMode()) {
            log.debug("Recommendation is forbidden of project({}), there's no need to clean acceleration tag", project);
            return;
        }

        log.info("start to clean acceleration tag by user");
        AsyncTaskManager manager = getInstance(project);
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) manager.get(ASYNC_ACCELERATION_TASK);
            AsyncAccelerationTask copied = manager.copyForWrite(asyncAcceleration);
            copied.getUserRefreshedTagMap().put(userName, false);
            manager.save(asyncAcceleration);
            return null;
        });
        log.info("clean acceleration tag successfully for project({}: by user {})", project, userName);
    }
}

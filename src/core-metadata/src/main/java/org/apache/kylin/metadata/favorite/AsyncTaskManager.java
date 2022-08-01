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
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncTaskManager {

    public static final String ASYNC_ACCELERATION_TASK = "async_acceleration_task";

    public static final Serializer<AsyncAccelerationTask> ASYNC_ACCELERATION_SERIALIZER = new JsonSerializer<>(
            AsyncAccelerationTask.class);

    private final KylinConfig kylinConfig;
    private final ResourceStore resourceStore;
    private final String resourceRoot;

    private AsyncTaskManager(KylinConfig kylinConfig, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            log.info("Initializing AccelerateTagManager with KylinConfig Id: {} for project {}",
                    System.identityHashCode(kylinConfig), project);
        this.kylinConfig = kylinConfig;
        resourceStore = ResourceStore.getKylinMetaStore(this.kylinConfig);
        this.resourceRoot = "/" + project + ResourceStore.ASYNC_TASK;
    }

    // called by reflection
    static AsyncTaskManager newInstance(KylinConfig config, String project) {
        return new AsyncTaskManager(config, project);
    }

    public static AsyncTaskManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, AsyncTaskManager.class);
    }

    private String path(String uuid) {
        return this.resourceRoot + "/" + uuid + MetadataConstants.FILE_SURFIX;
    }

    public void save(AsyncAccelerationTask asyncTask) {
        if (asyncTask.getTaskType().equalsIgnoreCase(ASYNC_ACCELERATION_TASK)) {
            resourceStore.checkAndPutResource(path(asyncTask.getUuid()), asyncTask, ASYNC_ACCELERATION_SERIALIZER);
        }
    }

    public AbstractAsyncTask get(String taskType) {
        List<AsyncAccelerationTask> asyncAccelerationTaskList = Lists.newArrayList();
        if (taskType.equalsIgnoreCase(ASYNC_ACCELERATION_TASK)) {
            asyncAccelerationTaskList = resourceStore.getAllResources(resourceRoot, ASYNC_ACCELERATION_SERIALIZER);
            if (asyncAccelerationTaskList.isEmpty()) {
                return new AsyncAccelerationTask(false, Maps.newHashMap(), ASYNC_ACCELERATION_TASK);
            }
        }
        return asyncAccelerationTaskList.get(0);
    }

    public static void resetAccelerationTagMap(String project) {
        log.info("reset acceleration tag for project({})", project);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) getInstance(
                    KylinConfig.getInstanceFromEnv(), project).get(ASYNC_ACCELERATION_TASK);
            asyncAcceleration.setAlreadyRunning(false);
            asyncAcceleration.setUserRefreshedTagMap(Maps.newHashMap());
            getInstance(KylinConfig.getInstanceFromEnv(), project).save(asyncAcceleration);
            return null;
        }, project);
        log.info("rest acceleration tag successfully for project({})", project);
    }

    public static void cleanAccelerationTagByUser(String project, String userName) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        if (!projectInstance.isSemiAutoMode()) {
            log.debug("Recommendation is forbidden of project({}), there's no need to clean acceleration tag", project);
            return;
        }

        if (!EpochManager.getInstance().checkEpochOwner(project) && !kylinConfig.isUTEnv()) {
            return;
        }

        log.info("start to clean acceleration tag by user");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            AsyncAccelerationTask asyncAcceleration = (AsyncAccelerationTask) getInstance(
                    KylinConfig.getInstanceFromEnv(), project).get(ASYNC_ACCELERATION_TASK);
            asyncAcceleration.getUserRefreshedTagMap().put(userName, false);
            getInstance(KylinConfig.getInstanceFromEnv(), project).save(asyncAcceleration);
            return null;
        }, project);
        log.info("clean acceleration tag successfully for project({}: by user {})", project, userName);
    }
}

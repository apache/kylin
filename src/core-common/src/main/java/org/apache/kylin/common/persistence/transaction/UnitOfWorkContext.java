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
package org.apache.kylin.common.persistence.transaction;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@RequiredArgsConstructor
public class UnitOfWorkContext {

    private final String project;

    private KylinConfig.SetAndUnsetThreadLocalConfig localConfig;
    private TransactionLock currentLock = null;

    @Delegate
    private UnitOfWorkParams params;

    List<UnitTask> onFinishedTasks = Lists.newArrayList();

    List<UnitTask> onUpdatedTasks = Lists.newArrayList();

    public void doAfterUnit(UnitTask task) {
        onFinishedTasks.add(task);
    }

    public void doAfterUpdate(UnitTask task) {
        onUpdatedTasks.add(task);
    }

    void cleanResource() {
        if (localConfig == null) {
            return;
        }

        KylinConfig config = localConfig.get();
        ResourceStore.clearCache(config);
        localConfig.close();
        localConfig = null;
    }

    void checkLockStatus() {
        Preconditions.checkNotNull(currentLock);
        Preconditions.checkState(currentLock.isHeldByCurrentThread());

    }

    void checkReentrant(UnitOfWorkParams params) {
        Preconditions.checkState(project.equals(params.getUnitName()) || this.params.isAll(),
                "re-entry of UnitOfWork with different unit name? existing: %s, new: %s", project,
                params.getUnitName());
        Preconditions.checkState(params.isReadonly() == isReadonly(),
                "re-entry of UnitOfWork with different lock type? existing: %s, new: %s", isReadonly(),
                params.isReadonly());
        Preconditions.checkState(params.isUseSandbox() == isUseSandbox(),
                "re-entry of UnitOfWork with different sandbox? existing: %s, new: %s", isReadonly(),
                params.isUseSandbox());
    }

    public void onUnitFinished() {
        onFinishedTasks.forEach(task -> {
            try {
                task.run();
            } catch (Exception e) {
                log.warn("Failed to run task after unit", e);
            }
        });
    }

    public void onUnitUpdated() {
        onUpdatedTasks.forEach(task -> {
            try {
                task.run();
            } catch (Exception e) {
                log.warn("Failed to run task after update metadata", e);
                throw new KylinException(CommonErrorCode.FAILED_UPDATE_METADATA, "task failed");
            }
        });
    }

    public interface UnitTask {
        void run() throws Exception;
    }
}

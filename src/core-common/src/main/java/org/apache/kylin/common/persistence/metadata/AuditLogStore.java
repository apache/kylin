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
package org.apache.kylin.common.persistence.metadata;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.transaction.AbstractAuditLogReplayWorker;

import lombok.val;

public interface AuditLogStore extends Closeable {

    void save(UnitMessages unitMessages);

    List<AuditLog> fetch(long currentId, long size);

    List<AuditLog> fetch(List<Long> auditIdList);

    long getMaxId();

    long getMinId();

    long getLogOffset();

    void restore(long currentId);

    void rotate();

    default void catchupWithTimeout() throws TimeoutException {
        val store = ResourceStore.getKylinMetaStore(getConfig());
        getReplayWorker().catchupFrom(store.getOffset());
        getReplayWorker().waitForCatchup(getMaxId(), getConfig().getCatchUpTimeout());
    }

    // If the current thread is in a transaction, the latest auditlogId cannot be obtained during
    // catchup because the transaction level is repeatable.
    // So, let's do the catchup in a new thread.
    default void catchupWithTimeoutInNewThread() {
        Thread catchupThread = new Thread(() -> {
            try {
                this.catchupWithTimeout();
            } catch (TimeoutException e) {
                throw new KylinRuntimeException(e);
            }
        });
        catchupThread.start();
        try {
            catchupThread.join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    default void catchupWithMaxTimeout() throws TimeoutException {
        val store = ResourceStore.getKylinMetaStore(getConfig());
        getReplayWorker().catchupFrom(store.getOffset());
        getReplayWorker().waitForCatchup(getMaxId(), getConfig().getCatchUpMaxTimeout());
    }

    default void catchup() {
        val store = ResourceStore.getKylinMetaStore(getConfig());
        getReplayWorker().catchupFrom(store.getOffset());
    }

    void setInstance(String instance);

    AuditLog get(String resPath, long mvcc);

    default void pause() {
        getReplayWorker().close(true);
    }

    default void reInit() {
        val store = ResourceStore.getKylinMetaStore(getConfig());
        getReplayWorker().reStartSchedule(store.getOffset());
    }

    long count(long startId, long endId);

    AbstractAuditLogReplayWorker getReplayWorker();

    KylinConfig getConfig();

    @Override
    default void close() throws IOException {
        getReplayWorker().close(false);
    }
}

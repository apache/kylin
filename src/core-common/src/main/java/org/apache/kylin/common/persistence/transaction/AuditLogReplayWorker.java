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

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.VersionConflictException;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.springframework.transaction.TransactionException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import lombok.Getter;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuditLogReplayWorker extends AbstractAuditLogReplayWorker {

    @Getter
    private volatile long logOffset = 0L;

    public void startSchedule(long currentId, boolean syncImmediately) {
        updateOffset(currentId);
        if (syncImmediately) {
            catchupInternal(1);
        }
        long interval = config.getCatchUpInterval();
        consumeExecutor.scheduleWithFixedDelay(() -> catchupInternal(1), interval, interval, TimeUnit.SECONDS);
    }

    public AuditLogReplayWorker(KylinConfig config, JdbcAuditLogStore restorer) {
        super(config, restorer);
    }

    public synchronized void updateOffset(long expected) {
        logOffset = Math.max(logOffset, expected);
    }

    public void forceUpdateOffset(long expected) {
        logOffset = expected;
    }

    @Override
    protected boolean hasCatch(long targetId) {
        return logOffset >= targetId;
    }

    @Override
    protected void catchupInternal(int countDown) {
        if (isStopped.get()) {
            log.info("Catchup Already stopped");
            return;
        }
        try {
            catchupToMaxId(logOffset);
        } catch (TransactionException | DatabaseNotAvailableException e) {
            log.warn("cannot create transaction or auditlog database connect error, ignore it", e);
            threadWait(5000);
        } catch (Exception e) {
            val rootCause = Throwables.getRootCause(e);
            if (rootCause instanceof VersionConflictException && countDown > 0) {
                handleConflictOnce((VersionConflictException) rootCause, countDown);
            } else if (rootCause instanceof InterruptedException) {
                log.info("may be canceled due to reload meta, skip this replay");
            } else {
                handleReloadAll(e);
            }
        }

    }

    private void threadWait(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    public void catchupToMaxId(final long currentId) {
        val replayer = MessageSynchronization.getInstance(config);
        val store = ResourceStore.getKylinMetaStore(config);
        replayer.setChecker(store.getChecker());

        val maxId = waitMaxIdOk(currentId);
        if (maxId == -1 || maxId == currentId) {
            return;
        }
        withTransaction(auditLogStore.getTransactionManager(), () -> {
            log.debug("start restore from {}, current max_id is {}", currentId, maxId);
            var start = currentId;
            while (start < maxId) {
                val logs = auditLogStore.fetch(start, Math.min(STEP, maxId - start));
                replayLogs(replayer, logs);
                start += STEP;
            }
            return maxId;
        });
        updateOffset(maxId);
    }

    private long waitMaxIdOk(long currentId) {
        try {
            val maxId = auditLogStore.getMaxId();
            if (maxId == currentId) {
                return maxId;
            }
            if (waitLogCommit(3, currentId, maxId)) {
                return maxId;
            }
            return -1;
        } catch (Exception e) {
            throw new DatabaseNotAvailableException(e);
        }
    }

    private boolean waitLogCommit(int maxTimes, long currentId, long maxId) {
        if (!config.isNeedReplayConsecutiveLog()) {
            return true;
        }
        int count = 0;
        while (!logAllCommit(currentId, maxId)) {
            threadWait(100);
            count++;
            if (count >= maxTimes) {
                return false;
            }
        }
        return true;
    }

    private void handleConflictOnce(VersionConflictException e, int countDown) {
        val replayer = MessageSynchronization.getInstance(config);
        val originResource = e.getResource();
        val targetResource = e.getTargetResource();
        val conflictedPath = originResource.getResPath();
        log.warn("Resource <{}:{}> version conflict, msg:{}", conflictedPath, originResource.getMvcc(), e.getMessage());
        log.info("Try to reload {}", originResource.getResPath());
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        val metaStore = resourceStore.getMetadataStore();
        try {
            val correctedResource = metaStore.load(conflictedPath);
            log.info("Origin version is {},  current version in store is {}", originResource.getMvcc(),
                    correctedResource.getMvcc());
            val fixResource = new AuditLog(0L, conflictedPath, correctedResource.getByteSource(),
                    correctedResource.getTimestamp(), originResource.getMvcc() + 1, null, null, null);
            replayer.replay(new UnitMessages(Lists.newArrayList(Event.fromLog(fixResource))));

            val currentAuditLog = resourceStore.getAuditLogStore().get(conflictedPath, targetResource.getMvcc());
            if (currentAuditLog != null) {
                log.info("After fix conflict, set offset to {}", currentAuditLog.getId());
                updateOffset(currentAuditLog.getId());
            }
        } catch (IOException ioException) {
            log.warn("Reload metadata <{}> failed", conflictedPath);
        }
        catchupInternal(countDown - 1);
    }

}

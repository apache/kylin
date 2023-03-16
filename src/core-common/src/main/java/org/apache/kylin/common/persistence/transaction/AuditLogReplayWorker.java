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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.VersionConflictException;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.springframework.transaction.TransactionException;

import org.apache.kylin.guava30.shaded.common.base.Joiner;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuditLogReplayWorker extends AbstractAuditLogReplayWorker {

    @Getter
    private volatile long logOffset = 0L;

    private final Queue<AuditLogReplayWorker.AuditIdTimeItem> delayIdQueue;

    private final long idEarliestTimeoutMills;
    private final long idTimeoutMills;
    private final int replayDelayBatch;

    public AuditLogReplayWorker(KylinConfig config, JdbcAuditLogStore restorer) {
        super(config, restorer);
        delayIdQueue = new ConcurrentLinkedQueue<>();
        idTimeoutMills = config.getEventualReplayDelayItemTimeout();
        replayDelayBatch = config.getEventualReplayDelayItemBatch();
        idEarliestTimeoutMills = TimeUnit.HOURS.toMillis(3);
    }

    public void startSchedule(long currentId, boolean syncImmediately) {
        updateOffset(currentId);
        delayIdQueue.clear();

        val minId = auditLogStore.getMinId();
        if (logOffset + 1 < minId) {
            log.warn("restore from currentId:{} + 1< minId:{} is irregular", currentId, minId);
        }
        if (syncImmediately) {
            catchupInternal(1);
        }
        long interval = config.getCatchUpInterval();
        consumeExecutor.scheduleWithFixedDelay(() -> catchupInternal(1), interval, interval, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void updateOffset(long expected) {
        if (expected > logOffset) {
            logOffset = expected;
        }
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
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
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
                delayIdQueue.clear();
                handleReloadAll(e);
            }
        }

    }

    private List<Long> collectReplayDelayedId(int maxCount) {
        if (delayIdQueue.isEmpty()) {
            return Lists.newArrayList();
        }

        List<Long> needReplayedIdList = Lists.newArrayList();
        val timeoutItemList = Lists.newArrayList();
        Iterator<AuditLogReplayWorker.AuditIdTimeItem> retryQueueIterator = delayIdQueue.iterator();
        long currentTime = System.currentTimeMillis();
        while (retryQueueIterator.hasNext()) {
            val idTimeItem = retryQueueIterator.next();
            needReplayedIdList.add(idTimeItem.getAuditLogId());
            if (idTimeItem.isTimeout(currentTime, idTimeoutMills)) {
                timeoutItemList.add(idTimeItem);
                retryQueueIterator.remove();
            }
            if (needReplayedIdList.size() >= maxCount) {
                break;
            }
        }

        if (CollectionUtils.isNotEmpty(timeoutItemList)) {
            log.warn("delay timeout id->{}", collectionToJoinString(timeoutItemList));
        }

        if (CollectionUtils.isEmpty(needReplayedIdList)) {
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                log.debug("needReplayedIdList is empty");
            }
            return Lists.newArrayList();
        }

        return needReplayedIdList;
    }

    private void fetchAndReplayDelayId(MessageSynchronization replayer, List<Long> needReplayedIdList) {
        if (CollectionUtils.isEmpty(needReplayedIdList)) {
            return;
        }

        try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
            val fetchAuditLog = auditLogStore.fetch(needReplayedIdList);
            if (CollectionUtils.isEmpty(fetchAuditLog)) {
                return;
            }
            log.debug("try replay delay id:{}", collectionToJoinString(needReplayedIdList));
            replayLogs(replayer, fetchAuditLog);

            val replaySuccessIdSet = fetchAuditLog.stream().map(AuditLog::getId).collect(Collectors.toSet());
            delayIdQueue.removeIf(winId -> replaySuccessIdSet.contains(winId.auditLogId));
            log.warn("finished replay delay id:{},queue:{}", collectionToJoinString(replaySuccessIdSet),
                    delayIdQueue.size());

        } catch (Exception e) {
            log.error("tryReplayDelayedId error", e);
            delayIdQueue.clear();
        }
    }

    public void catchupToMaxId(final long currentId) {
        val replayer = MessageSynchronization.getInstance(config);
        val store = ResourceStore.getKylinMetaStore(config);
        replayer.setChecker(store.getChecker());

        val needReplayedIdList = collectReplayDelayedId(replayDelayBatch);

        val currentWindow = new FixedWindow(currentId, auditLogStore.getMaxId());
        if (currentWindow.isEmpty() && CollectionUtils.isEmpty(needReplayedIdList)) {
            return;
        }

        val allCommitOk = waitMaxIdOk(currentWindow.getStart(), currentWindow.getEnd());

        val newOffset = withTransaction(auditLogStore.getTransactionManager(), () -> {
            fetchAndReplayDelayId(replayer, needReplayedIdList);

            if (currentWindow.isEmpty()) {
                return -1L;
            }

            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                log.debug("start restore from {}", currentWindow);
            }
            val stepWin = new SlideWindow(currentWindow);

            while (stepWin.forwardRightStep(STEP)) {
                val logs = auditLogStore.fetch(stepWin.getStart(), stepWin.length());
                replayLogs(replayer, logs);
                if (!allCommitOk) {
                    recordStepAbsentIdList(stepWin, logs);
                }
                stepWin.syncRightStep();
            }
            try (SetLogCategory ignored = new SetLogCategory(LogConstant.METADATA_CATEGORY)) {
                log.debug("end restore from {}, delay queue:{}", currentWindow, delayIdQueue.size());
            }
            return currentWindow.getEnd();
        });

        updateOffset(newOffset);
    }

    private boolean waitMaxIdOk(long currentId, long maxId) {
        try {
            if (maxId == currentId) {
                return true;
            }
            return waitLogCommit(replayWaitMaxRetryTimes, currentId, maxId);
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
            threadWait(replayWaitMaxTimeoutMills);
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

    private void recordStepAbsentIdList(FixedWindow stepWin, List<AuditLog> logs) {
        if (CollectionUtils.isEmpty(logs)) {
            return;
        }

        if (logs.size() == stepWin.length()) {
            return;
        }

        val replayedIdList = Lists.<Long> newArrayList();
        try {
            val curTime = System.currentTimeMillis();
            val latestTime = logs.stream().map(AuditLog::getTimestamp).max(Long::compareTo).orElse(curTime);
            if (curTime - latestTime > idEarliestTimeoutMills) {
                log.warn("skip too earliest id,{}->{}", curTime, latestTime);
                return;
            }

            replayedIdList.addAll(logs.stream().map(AuditLog::getId).collect(Collectors.toList()));
            val absentIdList = findAbsentId(replayedIdList, stepWin);
            log.warn("find absent id list:{},in {}", collectionToJoinString(absentIdList), stepWin);
            absentIdList.forEach(id -> delayIdQueue.add(new AuditLogReplayWorker.AuditIdTimeItem(id, curTime)));
        } catch (Exception e) {
            log.error("recordStepAbsentIdList:{},{} error", stepWin, collectionToJoinString(replayedIdList), e);
        }

    }

    /**
     * {(startId,endId]} - {replayedIdList}
     * @param replayedIdList
     * @return
     */
    @NonNull
    private List<Long> findAbsentId(List<Long> replayedIdList, FixedWindow fixedWindow) {
        if (CollectionUtils.isEmpty(replayedIdList)) {
            return Lists.newArrayList();
        }
        val replayedIdSet = new HashSet<>(replayedIdList);
        return LongStream.rangeClosed(fixedWindow.start + 1, fixedWindow.end).boxed()
                .filter(id -> !replayedIdSet.contains(id)).collect(Collectors.toList());
    }

    private static String collectionToJoinString(Collection<?> objects) {
        if (CollectionUtils.isEmpty(objects)) {
            return StringUtils.EMPTY;
        }
        return Joiner.on(",").join(objects);
    }

    @Getter
    static class AuditIdTimeItem {
        private final long auditLogId;
        private final long logTimestamp;

        public AuditIdTimeItem(long auditLogId, long logTimestamp) {
            this.auditLogId = auditLogId;
            this.logTimestamp = logTimestamp;
        }

        public boolean isTimeout(long currentTime, long timeout) {
            return currentTime - logTimestamp > timeout;
        }

        @Override
        public String toString() {
            return "[" + auditLogId + "," + logTimestamp + "]";
        }
    }

}

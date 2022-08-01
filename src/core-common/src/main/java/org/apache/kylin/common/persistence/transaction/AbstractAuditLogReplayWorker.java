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

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_CONNECT_META_DATABASE;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.Event;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;

import com.google.common.collect.Maps;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractAuditLogReplayWorker {

    protected static final long STEP = 1000;
    protected final JdbcAuditLogStore auditLogStore;
    protected final KylinConfig config;

    // only a thread is necessary
    protected volatile ScheduledExecutorService consumeExecutor = Executors.newScheduledThreadPool(1,
            new NamedThreadFactory("ReplayWorker"));

    protected final AtomicBoolean isStopped = new AtomicBoolean(false);

    protected AbstractAuditLogReplayWorker(KylinConfig config, JdbcAuditLogStore auditLogStore) {
        this.config = config;
        this.auditLogStore = auditLogStore;
    }

    public abstract void startSchedule(long currentId, boolean syncImmediately);

    public void catchup() {
        if (isStopped.get()) {
            return;
        }
        consumeExecutor.submit(() -> catchupInternal(1));
    }

    public void close(boolean isGracefully) {
        isStopped.set(true);
        if (isGracefully) {
            ExecutorServiceUtil.shutdownGracefully(consumeExecutor, 60);
        } else {
            ExecutorServiceUtil.forceShutdown(consumeExecutor);
        }
    }

    protected void replayLogs(MessageSynchronization replayer, List<AuditLog> logs) {
        Map<String, UnitMessages> messagesMap = Maps.newLinkedHashMap();
        for (AuditLog log : logs) {
            val event = Event.fromLog(log);
            String unitId = log.getUnitId();
            if (messagesMap.get(unitId) == null) {
                UnitMessages newMessages = new UnitMessages();
                newMessages.getMessages().add(event);
                messagesMap.put(unitId, newMessages);
            } else {
                messagesMap.get(unitId).getMessages().add(event);
            }
        }

        for (UnitMessages message : messagesMap.values()) {
            log.debug("replay {} event for project:{}", message.getMessages().size(), message.getKey());
            replayer.replay(message);
        }
    }

    public abstract long getLogOffset();

    public abstract void updateOffset(long expected);

    public abstract void forceUpdateOffset(long expected);

    protected abstract void catchupInternal(int countDown);

    protected abstract boolean hasCatch(long targetId);

    public void forceCatchFrom(long expected) {
        forceUpdateOffset(expected);
        catchup();
    }

    public void catchupFrom(long expected) {
        updateOffset(expected);
        catchup();
    }

    protected boolean logAllCommit(long startOffset, long endOffset) {
        return auditLogStore.count(startOffset, endOffset) == (endOffset - startOffset);
    }

    protected void handleReloadAll(Exception e) {
        log.error("Critical exception happened, try to reload metadata ", e);
        try {
            MessageSynchronization messageSynchronization = MessageSynchronization
                    .getInstance(KylinConfig.getInstanceFromEnv());
            messageSynchronization.replayAllMetadata(false);
        } catch (Throwable th) {
            log.error("reload all failed", th);
        }
        log.info("Reload finished");
    }

    public void waitForCatchup(long targetId, long timeout) throws TimeoutException {
        long endTime = System.currentTimeMillis() + timeout * 1000;
        try {
            while (System.currentTimeMillis() < endTime) {
                if (hasCatch(targetId)) {
                    return;
                }
                Thread.sleep(50);
            }
        } catch (Exception e) {
            log.info("Wait for catchup to {} failed", targetId, e);
            Thread.currentThread().interrupt();
        }
        throw new TimeoutException(String.format(Locale.ROOT, "Cannot reach %s before %s, current is %s", targetId,
                endTime, getLogOffset()));
    }

    public void reStartSchedule(long currentId) {
        if (!isStopped.get()) {
            log.info("replayer is running , don't need restart");
            return;
        }
        isStopped.set(false);
        consumeExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("ReplayWorker"));
        startSchedule(currentId, false);
    }

    public static class StartReloadEvent {
    }

    public static class EndReloadEvent {
    }

    protected static class DatabaseNotAvailableException extends KylinException {
        public DatabaseNotAvailableException(Exception e) {
            super(FAILED_CONNECT_META_DATABASE, e);
        }
    }

}

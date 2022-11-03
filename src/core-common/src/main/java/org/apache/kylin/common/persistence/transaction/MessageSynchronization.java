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

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.scheduler.EventBusFactory;

import com.google.common.collect.Lists;

import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageSynchronization {

    private final KylinConfig config;
    private final EventListenerRegistry eventListener;
    @Setter
    private ResourceStore.Callback<Boolean> checker;

    public static MessageSynchronization getInstance(KylinConfig config) {
        return config.getManager(MessageSynchronization.class);
    }

    static MessageSynchronization newInstance(KylinConfig config) {
        return new MessageSynchronization(config);
    }

    private MessageSynchronization(KylinConfig config) {
        this.config = config;
        eventListener = EventListenerRegistry.getInstance(config);
    }

    public void replay(UnitMessages messages) {
        if (messages.isEmpty()) {
            return;
        }

        if (checker != null && checker.check(messages)) {
            return;
        }

        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().processor(() -> {
            if (Thread.interrupted()) {
                throw new InterruptedException("skip this replay");
            }
            replayInTransaction(messages);
            return null;
        }).maxRetry(1).unitName(messages.getKey()).useSandbox(false).build());
    }

    void replayInTransaction(UnitMessages messages) {
        UnitOfWork.replaying.set(true);
        messages.getMessages().forEach(event -> {
            if (event instanceof ResourceCreateOrUpdateEvent) {
                replayUpdate((ResourceCreateOrUpdateEvent) event);
                eventListener.onUpdate((ResourceCreateOrUpdateEvent) event);
            } else if (event instanceof ResourceDeleteEvent) {
                replayDelete((ResourceDeleteEvent) event);
                eventListener.onDelete((ResourceDeleteEvent) event);
            }
        });
        UnitOfWork.replaying.remove();
    }

    private void replayDelete(ResourceDeleteEvent event) {
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        log.trace("replay delete for res {}", event.getResPath());
        resourceStore.deleteResource(event.getResPath());
    }

    private void replayUpdate(ResourceCreateOrUpdateEvent event) {
        val resourceStore = ResourceStore.getKylinMetaStore(config);
        log.trace("replay update for res {}, with new version: {}", event.getResPath(),
                event.getCreatedOrUpdated().getMvcc());
        val raw = event.getCreatedOrUpdated();
        val oldRaw = resourceStore.getResource(raw.getResPath());
        if (!config.isJobNode()) {
            resourceStore.putResourceWithoutCheck(raw.getResPath(), raw.getByteSource(), raw.getTimestamp(),
                    raw.getMvcc());
            return;
        }

        if (oldRaw == null) {
            resourceStore.putResourceWithoutCheck(raw.getResPath(), raw.getByteSource(), raw.getTimestamp(),
                    raw.getMvcc());
        } else {
            resourceStore.checkAndPutResource(raw.getResPath(), raw.getByteSource(), raw.getTimestamp(),
                    raw.getMvcc() - 1);
        }
    }

    public void replayAllMetadata(boolean needCloseReplay) throws IOException {
        val lockKeys = Lists.newArrayList(TransactionLock.getProjectLocksForRead().keySet());
        lockKeys.sort(Comparator.naturalOrder());
        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        try {
            EventBusFactory.getInstance().postSync(new AuditLogReplayWorker.StartReloadEvent());
            if (needCloseReplay) {
                resourceStore.getAuditLogStore().pause();
            }
            for (String lockKey : lockKeys) {
                TransactionLock.getLock(lockKey, false).lock();
            }
            log.info("Acquired all locks, start to reload");

            resourceStore.reload();
            log.info("Reload finished");
        } finally {
            Collections.reverse(lockKeys);
            for (String lockKey : lockKeys) {
                TransactionLock.getLock(lockKey, false).unlock();
            }
            if (needCloseReplay) {
                // if not stop, reinit return directly
                resourceStore.getAuditLogStore().reInit();
            } else {
                // for update offset of auditlog
                resourceStore.getAuditLogStore().catchup();
            }
            EventBusFactory.getInstance().postSync(new AuditLogReplayWorker.EndReloadEvent());
        }
    }

}

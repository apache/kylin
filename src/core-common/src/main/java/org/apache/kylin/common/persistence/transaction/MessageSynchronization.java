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

import static org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore.HDFS_SCHEME;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.persistence.metadata.FileSystemMetadataStore;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;

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

        replayInTransaction(messages);
    }

    synchronized void replayInTransaction(UnitMessages messages) {
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
        val resPath = event.getResPath();
        val oldRaw = resourceStore.getResource(resPath);
        if (!config.isJobNode() && raw.getContentDiff() == null) {
            resourceStore.putResourceWithoutCheck(resPath, raw.getByteSource(), raw.getTs(), raw.getMvcc());
            return;
        }

        if (oldRaw == null) {
            if (raw.getContentDiff() != null) {
                throw new IllegalStateException("No pre-update data found, unable to calculate json diff! metaKey: "
                        + raw.getMetaKey());
            }
            resourceStore.putResourceWithoutCheck(resPath, raw.getByteSource(), raw.getTs(), raw.getMvcc());
        } else {
            ByteSource byteSource = raw.getContentDiff() != null ? RawResource.applyContentDiffFromRaw(oldRaw, raw)
                    : raw.getByteSource();
            if (resourceStore.getMetadataStore() instanceof FileSystemMetadataStore) {
                resourceStore.putResourceWithoutCheck(resPath, byteSource, raw.getTs(), raw.getMvcc(), true);
            } else {
                resourceStore.checkAndPutResource(resPath, byteSource, raw.getTs(), raw.getMvcc() - 1);
            }
        }
    }

    public void replayAllMetadata(boolean needCloseReplay) throws IOException {
        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        String curMetaUrl = null;
        boolean needResetUrl = !config.isJobNode() && config.getMetadataStoreType().equals(HDFS_SCHEME)
                && !config.isAuditLogOnlyOriginalEnabled();
        if (needResetUrl) {
            curMetaUrl = config.getMetadataUrl().toString();
            // For hdfs, use scheme of jdbc to reload because json diff cannot do exception recovery based
            // on hdfs reload results
            config.setMetadataUrl(config.getCoreMetadataDBUrl().toString());
            log.info("Replay all metadata by jdbc url");
        }
        try {
            EventBusFactory.getInstance().postSync(new AuditLogReplayWorker.StartReloadEvent());
            if (needCloseReplay) {
                resourceStore.getAuditLogStore().pause();
            }
            // TODO lockAll
            log.info("Acquired all locks, start to reload");

            resourceStore.reload();
            log.info("Reload finished");
        } finally {
            if (needResetUrl) {
                config.setMetadataUrl(curMetaUrl);
                log.info("Finished replay, reset metadata url to {}", curMetaUrl);
            }
            // TODO unlock
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

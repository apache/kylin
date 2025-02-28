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

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.AuditLog;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.UnitMessages;
import org.apache.kylin.common.persistence.event.ResourceCreateOrUpdateEvent;
import org.apache.kylin.common.persistence.event.ResourceDeleteEvent;
import org.apache.kylin.common.persistence.transaction.AuditLogReplayWorker;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.springframework.security.core.context.SecurityContextHolder;

import lombok.Getter;
import lombok.val;

public class MemoryAuditLogStore implements AuditLogStore {
    
    private String instance = AddressUtil.getLocalInstance();
    private final List<AuditLog> logs = new ArrayList<>();
    
    private final AtomicLong maxId = new AtomicLong(0L);

    @Getter
    private final AuditLogReplayWorker replayWorker;

    @Getter
    private final KylinConfig config;

    public MemoryAuditLogStore(KylinConfig config) {
        this.config = config;
        this.replayWorker = new AuditLogReplayWorker(config, this);
    }

    @Override
    public void save(UnitMessages unitMessages) {
        val unitId = unitMessages.getUnitId();
        val operator = Optional.ofNullable(SecurityContextHolder.getContext().getAuthentication())
                .map(Principal::getName).orElse(null);
        List<AuditLog> newLogs = unitMessages.getMessages().stream().map(event -> {
            long id = maxId.addAndGet(1);
            if (event instanceof ResourceCreateOrUpdateEvent) {
                ResourceCreateOrUpdateEvent e = (ResourceCreateOrUpdateEvent) event;
                RawResource raw = e.getCreatedOrUpdated();
                return new AuditLog(id, e.getResPath(), ByteSource.wrap(e.getMetaContent()), raw.getTs(), raw.getMvcc(),
                        unitId, raw.getModelUuid(), operator, instance, raw.getProject(),
                        raw.getContentDiff() != null);
            } else if (event instanceof ResourceDeleteEvent) {
                ResourceDeleteEvent e = (ResourceDeleteEvent) event;
                return new AuditLog(id, e.getResPath(), null, System.currentTimeMillis(), null, unitId,
                        null, operator, instance, e.getKey(), false);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toList());
        logs.addAll(newLogs);
    }

    @Override
    public List<AuditLog> fetch(long currentId, long size) {
        return logs.stream().filter(log -> log.getId() > currentId && log.getId() <= currentId + size)
                .collect(Collectors.toList());
    }

    @Override
    public List<AuditLog> fetch(List<Long> auditIdList) {
        return logs.stream().filter(log -> auditIdList.contains(log.getId())).collect(Collectors.toList());
    }

    @Override
    public long getMaxId() {
        return maxId.get();
    }

    @Override
    public long getMinId() {
        return logs.isEmpty() ? 0 : 1;
    }

    @Override
    public long getLogOffset() {
        return replayWorker.getLogOffset();
    }

    @Override
    public void restore(long currentId) {
        // Do nothing
    }

    @Override
    public void rotate() {
        //Do nothing, MemoryAuditLog no need to rotate.
    }

    @Override
    public void setInstance(String instance) {
        this.instance = instance;
    }

    @Override
    public AuditLog get(String resPath, long mvcc) {
        return logs.stream().filter(log -> log.getResPath().equals(resPath) && log.getMvcc() == mvcc).findFirst()
                .orElse(null);
    }

    @Override
    public long count(long startId, long endId) {
        return logs.stream().filter(log -> log.getId() > startId && log.getId() <= endId).count();
    }
}

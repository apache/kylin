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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.InMemResourceStore;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.ITransactionManager;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.Pair;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileTransactionHelper implements ITransactionManager {
    
    private final FileSystemMetadataStore store;

    public FileTransactionHelper(FileSystemMetadataStore store) {
        this.store = store;
    }

    private final ThreadLocal<Set<ReentrantLock>> OWNED_LOCKS = ThreadLocal.withInitial(HashSet::new);
    private final ConcurrentHashMap<String, ReentrantLock> LOCK_MAP = new ConcurrentHashMap<>();

    @Override
    public TransactionStatus getTransaction() throws TransactionException {
        return new SimpleTransactionStatus();
    }

    @Override
    public void commit(TransactionStatus status) throws TransactionException {
        releaseOwnedLocks();
    }

    @Override
    public void rollback(TransactionStatus status) throws TransactionException {
        KylinConfig conf = KylinConfig.readSystemKylinConfig();
        InMemResourceStore mem = (InMemResourceStore) ResourceStore.getKylinMetaStore(conf);
        Set<String> changesResource = UnitOfWork.get().getCopyForWriteItems();
        for (String resPath : changesResource) {
            RawResource raw = mem.getResource(resPath);
            Pair<MetadataType, String> typeAndKey = MetadataType.splitKeyWithType(resPath);
            if (raw == null) {
                raw = RawResource.constructResource(typeAndKey.getFirst(), null, 0, -1, typeAndKey.getSecond());
            }
            store.save(typeAndKey.getFirst(), raw);
            log.info("Rollback metadata for FileSystemMetadataStore: " + resPath);
        }
        releaseOwnedLocks();
    }

    public void lockResource(String lockPath) {
        ReentrantLock lock = LOCK_MAP.computeIfAbsent(lockPath, k -> new ReentrantLock());
        log.debug("LOCK: try to lock path " + lockPath);
        if (OWNED_LOCKS.get().contains(lock)) {
            log.debug("LOCK: already locked " + lockPath);
        } else {
            try {
                if (!lock.tryLock(1, TimeUnit.MINUTES)) {
                    log.debug("LOCK: failed to lock for " + lockPath);
                    throw new LockTimeoutException(lockPath);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KylinRuntimeException(e);
            }
            log.debug("LOCK: locked " + lockPath);
            OWNED_LOCKS.get().add(lock);
        }
    }

    private void releaseOwnedLocks() {
        OWNED_LOCKS.get().forEach(ReentrantLock::unlock);
        log.debug("LOCK: release lock count " + OWNED_LOCKS.get().size());
        OWNED_LOCKS.get().clear();
    }

    static class LockTimeoutException extends RuntimeException {
        public LockTimeoutException(String lockUnit) {
            super(lockUnit);
        }
    }
}

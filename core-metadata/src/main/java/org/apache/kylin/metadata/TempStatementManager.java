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

package org.apache.kylin.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TempStatementManager {
    private static final Logger logger = LoggerFactory.getLogger(TempStatementManager.class);

    public static TempStatementManager getInstance(KylinConfig config) {
        return config.getManager(TempStatementManager.class);
    }

    // called by reflection
    static TempStatementManager newInstance(KylinConfig config) throws IOException {
        return new TempStatementManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private CaseInsensitiveStringCache<TempStatementEntity> tmpStatMap;
    private CachedCrudAssist<TempStatementEntity> crud;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    private TempStatementManager(KylinConfig cfg) throws IOException {
        this.config = cfg;
        this.tmpStatMap = new CaseInsensitiveStringCache<>(config, "temp_statement");
        this.crud = new CachedCrudAssist<TempStatementEntity>(getStore(), ResourceStore.TEMP_STATMENT_RESOURCE_ROOT,
                TempStatementEntity.class, tmpStatMap) {
            @Override
            protected TempStatementEntity initEntityAfterReload(TempStatementEntity t, String resourceName) {
                return t; // noop
            }
        };

        crud.reloadAll();

        // touch lower level metadata before registering my listener
        Broadcaster.getInstance(config).registerListener(new TempStatementSyncListener(), "temp_statement");
    }

    private class TempStatementSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            try (AutoLock l = lock.lockForWrite()) {
                if (event == Broadcaster.Event.DROP)
                    tmpStatMap.removeLocal(cacheKey);
                else
                    crud.reloadQuietly(cacheKey);
            }
        }
    }

    public String getTempStatement(String statementId) {
        return getTempStatement(TempStatementEntity.DEFAULT_SESSION_ID, statementId);
    }

    public String getTempStatement(String sessionId, String statementId) {
        TempStatementEntity entity = getTempStatEntity(sessionId, statementId);
        return entity == null ? null : entity.statement;
    }

    public TempStatementEntity getTempStatEntity(String sessionId, String statementId) {
        try (AutoLock l = lock.lockForRead()) {
            return tmpStatMap.get(TempStatementEntity.resourceName(sessionId, statementId));
        }
    }

    // for test
    List<String> reloadAllTempStatement() throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            crud.reloadAll();
            return new ArrayList<>(tmpStatMap.keySet());
        }
    }

    public void updateTempStatement(String statementId, String statement) throws IOException {
        updateTempStatement(TempStatementEntity.DEFAULT_SESSION_ID, statementId, statement);
    }

    public void updateTempStatement(String sessionId, String statementId, String statement) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            TempStatementEntity entity = new TempStatementEntity(sessionId, statementId, statement);
            entity = prepareToOverwrite(entity, getTempStatEntity(sessionId, statementId));
            updateTempStatementWithRetry(entity, 0);
        }
    }

    private TempStatementEntity prepareToOverwrite(TempStatementEntity entity, TempStatementEntity origin) {
        if (origin == null) {
            // create
            entity.updateRandomUuid();
        } else {
            // update
            entity.setUuid(origin.getUuid());
            entity.setLastModified(origin.getLastModified());
        }
        return entity;
    }

    private void updateTempStatementWithRetry(TempStatementEntity entity, int retry) throws IOException {
        try {
            crud.save(entity);
        } catch (WriteConflictException ise) {
            logger.warn("Write conflict to update temp statement" + entity.statementId + " at try " + retry
                    + ", will retry...");
            if (retry >= 7) {
                logger.error("Retried 7 times till got error, abandoning...", ise);
                throw ise;
            }

            TempStatementEntity reload = crud.reload(entity.resourceName());
            entity = prepareToOverwrite(entity, reload);
            updateTempStatementWithRetry(entity, ++retry);
        }
    }

    public void removeTempStatement(String statementId) throws IOException {
        removeTempStatement(TempStatementEntity.DEFAULT_SESSION_ID, statementId);
    }

    public void removeTempStatement(String session, String statementId) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            crud.delete(TempStatementEntity.resourceName(session, statementId));
        }
    }

    private ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    @SuppressWarnings({ "serial", "unused" })
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
    private static class TempStatementEntity extends RootPersistentEntity {
        private static final String DEFAULT_SESSION_ID = "DEFAULT_SESSION";
        @JsonProperty("session_id")
        private String sessionId;
        @JsonProperty("statement_id")
        private String statementId;
        @JsonProperty("statement")
        private String statement;

        // for jackson
        public TempStatementEntity() {
        }

        public TempStatementEntity(String statementId, String statement) {
            this(DEFAULT_SESSION_ID, statementId, statement);
        }

        public TempStatementEntity(String sessionId, String statementId, String statement) {
            this.sessionId = sessionId;
            this.statementId = statementId;
            this.statement = statement;
        }

        public String getStatementId() {
            return statementId;
        }

        public void setStatementId(String statementId) {
            this.statementId = statementId;
        }

        public String getStatement() {
            return statement;
        }

        public void setStatement(String statement) {
            this.statement = statement;
        }

        /**
         * Get the key to localmapping
         * @return
         */
        public String getMapKey() {
            return resourceName();
        }

        @Override
        public String resourceName() {
            return sessionId + "/" + statementId;
        }

        public static String resourceName(String sessionId, String statementId) {
            return sessionId + "/" + statementId;
        }

        public String concatResourcePath() {
            return concatResourcePath(this.sessionId, this.statementId);
        }

        public static String concatResourcePath(String statementId) {
            return concatResourcePath(DEFAULT_SESSION_ID, statementId);
        }

        public static String concatResourcePath(String sessionId, String statementId) {
            return ResourceStore.TEMP_STATMENT_RESOURCE_ROOT + "/" + sessionId + "/" + statementId
                    + MetadataConstants.FILE_SURFIX;
        }
    }
}

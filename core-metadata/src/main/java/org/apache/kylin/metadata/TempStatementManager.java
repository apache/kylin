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
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TempStatementManager {
    private static final Logger logger = LoggerFactory.getLogger(TempStatementManager.class);
    public static final Serializer<TempStatementEntity> TEMP_STATEMENT_SERIALIZER = new JsonSerializer<>(
            TempStatementEntity.class);

    public static TempStatementManager getInstance(KylinConfig config) {
        return config.getManager(TempStatementManager.class);
    }

    // called by reflection
    static TempStatementManager newInstance(KylinConfig config) throws IOException {
        return new TempStatementManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private CaseInsensitiveStringCache<String> tempStatementMap;

    private TempStatementManager(KylinConfig config) throws IOException {
        init(config);
    }

    private void init(KylinConfig config) throws IOException {
        this.config = config;
        this.tempStatementMap = new CaseInsensitiveStringCache<>(config, "temp_statement");

        reloadAllTempStatement();

        // touch lower level metadata before registering my listener
        Broadcaster.getInstance(config).registerListener(new TempStatementSyncListener(), "temp_statement");
    }

    private void reloadAllTempStatement() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading temp statement from folder "
                + store.getReadableResourcePath(ResourceStore.TEMP_STATMENT_RESOURCE_ROOT));

        tempStatementMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TEMP_STATMENT_RESOURCE_ROOT,
                MetadataConstants.FILE_SURFIX);
        for (String path : paths) {
            reloadTempStatementAt(path);
        }

        logger.debug("Loaded " + tempStatementMap.size() + " Temp Statement(s)");
    }

    private TempStatementEntity reloadTempStatement(String statementId) throws IOException {
        return reloadTempStatement(TempStatementEntity.DEFAULT_SESSION_ID, statementId);
    }

    private TempStatementEntity reloadTempStatement(String sessionId, String statementId) throws IOException {
        return reloadTempStatementAt(TempStatementEntity.concatResourcePath(sessionId, statementId));
    }

    private TempStatementEntity reloadTempStatementAt(String path) throws IOException {
        ResourceStore store = getStore();

        TempStatementEntity s = store.getResource(path, TempStatementEntity.class, TEMP_STATEMENT_SERIALIZER);
        if (s == null) {
            return null;
        }

        tempStatementMap.putLocal(s.getMapKey(), s.getStatement());
        return s;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public String getTempStatement(String statementId) {
        return getTempStatement(TempStatementEntity.DEFAULT_SESSION_ID, statementId);
    }

    public String getTempStatement(String sessionId, String statementId) {
        return tempStatementMap.get(TempStatementEntity.getMapKey(sessionId, statementId));
    }
    // for test
    List<String> listAllTempStatement() throws IOException {
        reloadAllTempStatement();
        return new ArrayList<>(tempStatementMap.keySet());
    }

    public void updateTempStatement(String statementId, String statement) throws IOException {
        updateTempStatement(TempStatementEntity.DEFAULT_SESSION_ID, statementId, statement);
    }

    public void updateTempStatement(String sessionId, String statementId, String statement) throws IOException {
        TempStatementEntity entity = new TempStatementEntity(sessionId, statementId, statement);
        updateTempStatementWithRetry(entity, 0);
        tempStatementMap.put(entity.getMapKey(), statement);
    }

    public void updateTempStatementWithRetry(TempStatementEntity entity, int retry) throws IOException {
        ResourceStore store = getStore();
        try {
            store.putResource(entity.concatResourcePath(), entity, TEMP_STATEMENT_SERIALIZER);
        } catch (IllegalStateException ise) {
            logger.warn("Write conflict to update temp statement" + entity.statementId + " at try " + retry
                    + ", will retry...");
            if (retry >= 7) {
                logger.error("Retried 7 times till got error, abandoning...", ise);
                throw ise;
            }

            TempStatementEntity reload = reloadTempStatement(entity.statementId);
            reload.setStatement(entity.statement);
            retry++;
            updateTempStatementWithRetry(reload, retry);
        }
    }
    public void removeTempStatement(String statementId) throws IOException {
        removeTempStatement(TempStatementEntity.DEFAULT_SESSION_ID, statementId);
    }

    public void removeTempStatement(String session, String statementId) throws IOException {
        ResourceStore store = getStore();
        store.deleteResource(TempStatementEntity.concatResourcePath(session, statementId));
        tempStatementMap.remove(TempStatementEntity.concatResourcePath(session, statementId));
    }

    private class TempStatementSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            if (event == Broadcaster.Event.DROP)
                tempStatementMap.removeLocal(cacheKey);
            else
                reloadTempStatementAt(TempStatementEntity.concatResourcePath(cacheKey));
        }
    }

    @SuppressWarnings("serial")
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
            return sessionId + "/" + statementId;
        }

        public static String getMapKey(String sessionId, String statementId) {
            return sessionId + "/" + statementId;
        }

        public String concatResourcePath() {
            return concatResourcePath(this.sessionId, this.statementId);
        }

        public static String concatResourcePath(String statementId) {
            return concatResourcePath(DEFAULT_SESSION_ID, statementId);
        }

        public static String concatResourcePath(String sessionId, String statementId) {
            return ResourceStore.TEMP_STATMENT_RESOURCE_ROOT + "/" + sessionId + "/" + statementId + MetadataConstants.FILE_SURFIX;
        }
    }
}

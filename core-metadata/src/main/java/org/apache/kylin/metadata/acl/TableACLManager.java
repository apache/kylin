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

package org.apache.kylin.metadata.acl;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.common.util.AutoReadWriteLock.AutoLock;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.Broadcaster.Event;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class TableACLManager {

    private static final Logger logger = LoggerFactory.getLogger(TableACLManager.class);

    public static TableACLManager getInstance(KylinConfig config) {
        return config.getManager(TableACLManager.class);
    }

    // called by reflection
    static TableACLManager newInstance(KylinConfig config) throws IOException {
        return new TableACLManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    // user ==> TableACL
    private CaseInsensitiveStringCache<TableACL> tableACLMap;
    private CachedCrudAssist<TableACL> crud;
    private AutoReadWriteLock lock = new AutoReadWriteLock();

    public TableACLManager(KylinConfig config) throws IOException {
        logger.info("Initializing TableACLManager with config " + config);
        this.config = config;
        this.tableACLMap = new CaseInsensitiveStringCache<>(config, "table_acl");
        this.crud = new CachedCrudAssist<TableACL>(getStore(), "/table_acl", "", TableACL.class, tableACLMap, true) {
            @Override
            protected TableACL initEntityAfterReload(TableACL acl, String resourceName) {
                acl.init(resourceName);
                return acl;
            }
        };

        crud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new TableACLSyncListener(), "table_acl");
    }

    private class TableACLSyncListener extends Broadcaster.Listener {

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            try (AutoLock l = lock.lockForWrite()) {
                if (event == Event.DROP)
                    tableACLMap.removeLocal(cacheKey);
                else
                    crud.reloadQuietly(cacheKey);
            }
            broadcaster.notifyProjectACLUpdate(cacheKey);
        }
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public TableACL getTableACLByCache(String project) {
        try (AutoLock l = lock.lockForRead()) {
            TableACL tableACL = tableACLMap.get(project);
            if (tableACL == null) {
                return newTableACL(project);
            }
            return tableACL;
        }
    }

    public void addTableACL(String project, String name, String table, String type) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            TableACL tableACL = loadTableACL(project).add(name, table, type);
            crud.save(tableACL);
        }
    }

    public void deleteTableACL(String project, String name, String table, String type) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            TableACL tableACL = loadTableACL(project).delete(name, table, type);
            crud.save(tableACL);
        }
    }

    public void deleteTableACL(String project, String name, String type) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            TableACL tableACL = loadTableACL(project).delete(name, type);
            crud.save(tableACL);
        }
    }

    public void deleteTableACLByTbl(String project, String table) throws IOException {
        try (AutoLock l = lock.lockForWrite()) {
            TableACL tableACL = loadTableACL(project).deleteByTbl(table);
            crud.save(tableACL);
        }
    }

    private TableACL loadTableACL(String project) throws IOException {
        TableACL acl = crud.reload(project);
        if (acl == null) {
            acl = newTableACL(project);
        }
        return acl;
    }

    private TableACL newTableACL(String project) {
        TableACL acl = new TableACL();
        acl.updateRandomUuid();
        acl.init(project);
        return acl;
    }

}

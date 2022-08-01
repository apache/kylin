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

package org.apache.kylin.query.blacklist;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class SQLBlacklistManager {

    private static final Logger logger = LoggerFactory.getLogger(SQLBlacklistManager.class);

    private KylinConfig config;

    private CachedCrudAssist<SQLBlacklist> crud;

    public SQLBlacklistManager(KylinConfig config) {
        if (!UnitOfWork.isAlreadyInTransaction()) {
            logger.info("Initializing SQLBlacklistManager with KylinConfig Id: {}", System.identityHashCode(config));
        }
        logger.info("Initializing SQLBlacklistManager with config {}", config);
        this.config = config;
        this.crud = new CachedCrudAssist<SQLBlacklist>(getStore(), SQLBlacklist.SQL_BLACKLIST_RESOURCE_ROOT,
                SQLBlacklist.class) {
            @Override
            public SQLBlacklist initEntityAfterReload(SQLBlacklist sqlBlacklist, String resourceName) {
                return sqlBlacklist;
            }
        };

        crud.reloadAll();
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.config);
    }

    public static SQLBlacklistManager getInstance(KylinConfig config) {
        return config.getManager(SQLBlacklistManager.class);
    }

    // called by reflection
    @SuppressWarnings("unused")
    static SQLBlacklistManager newInstance(KylinConfig config) {
        try {
            String cls = SQLBlacklistManager.class.getName();
            Class<? extends SQLBlacklistManager> clz = ClassUtil.forName(cls, SQLBlacklistManager.class);
            return clz.getConstructor(KylinConfig.class).newInstance(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init SQLBlacklistManager from " + config, e);
        }
    }

    public SQLBlacklistItem getSqlBlacklistItemById(String project, String id) {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        return sqlBlacklist.getSqlBlacklistItem(id);
    }

    public SQLBlacklistItem getSqlBlacklistItemByRegex(String project, String regex) {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        return sqlBlacklist.getSqlBlacklistItemByRegex(regex);
    }

    public SQLBlacklistItem getSqlBlacklistItemBySql(String project, String sql) {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        return sqlBlacklist.getSqlBlacklistItemBySql(sql);
    }

    public SQLBlacklist getSqlBlacklist(String project) {
        return crud.get(project);
    }

    public SQLBlacklist saveSqlBlacklist(SQLBlacklist sqlBlacklist) throws IOException {
        SQLBlacklist savedSqlBlacklist = getSqlBlacklist(sqlBlacklist.getProject());
        if (null != savedSqlBlacklist) {
            savedSqlBlacklist.setBlacklistItems(sqlBlacklist.getBlacklistItems());
            crud.save(savedSqlBlacklist);
        } else {
            crud.save(sqlBlacklist);
        }
        return sqlBlacklist;
    }

    public SQLBlacklist addSqlBlacklistItem(String project, SQLBlacklistItem sqlBlacklistItem) throws IOException {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            sqlBlacklist = new SQLBlacklist();
            sqlBlacklist.setProject(project);
        }
        sqlBlacklist.addBlacklistItem(sqlBlacklistItem);
        crud.save(sqlBlacklist);
        return sqlBlacklist;
    }

    public SQLBlacklist updateSqlBlacklistItem(String project, SQLBlacklistItem sqlBlacklistItem) throws IOException {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        SQLBlacklistItem originItem = sqlBlacklist.getSqlBlacklistItem(sqlBlacklistItem.getId());
        if (null == originItem) {
            return sqlBlacklist;
        }
        originItem.setRegex(sqlBlacklistItem.getRegex());
        originItem.setSql(sqlBlacklistItem.getSql());
        originItem.setConcurrentLimit(sqlBlacklistItem.getConcurrentLimit());
        crud.save(sqlBlacklist);
        return sqlBlacklist;
    }

    public SQLBlacklist deleteSqlBlacklistItem(String project, String id) throws IOException {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        sqlBlacklist.deleteSqlBlacklistItem(id);
        crud.save(sqlBlacklist);
        return sqlBlacklist;
    }

    public SQLBlacklist clearBlacklist(String project) throws IOException {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        sqlBlacklist.setBlacklistItems(Lists.newArrayList());
        crud.save(sqlBlacklist);
        return sqlBlacklist;
    }

    public SQLBlacklistItem matchSqlBlacklist(String project, String sql) {
        SQLBlacklist sqlBlacklist = getSqlBlacklist(project);
        if (null == sqlBlacklist) {
            return null;
        }
        return sqlBlacklist.match(sql);
    }
}

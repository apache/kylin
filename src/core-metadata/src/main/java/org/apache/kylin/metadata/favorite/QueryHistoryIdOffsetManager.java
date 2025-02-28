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

package org.apache.kylin.metadata.favorite;

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

public class QueryHistoryIdOffsetManager {

    public static List<QueryHistoryIdOffset.OffsetType> ALL_OFFSET_TYPE = Arrays
            .asList(QueryHistoryIdOffset.OffsetType.META, QueryHistoryIdOffset.OffsetType.ACCELERATE);

    private final QueryHistoryIdOffsetStore jdbcIdOffsetStore;
    private final String project;

    @SuppressWarnings("unused")
    private QueryHistoryIdOffsetManager(String project) throws Exception {
        this.project = project;
        this.jdbcIdOffsetStore = new QueryHistoryIdOffsetStore(KylinConfig.getInstanceFromEnv());
    }

    public static QueryHistoryIdOffsetManager getInstance(String project) {
        return Singletons.getInstance(project, QueryHistoryIdOffsetManager.class);
    }

    public DataSourceTransactionManager getTransactionManager() {
        return jdbcIdOffsetStore.getTransactionManager();
    }

    public void saveOrUpdate(QueryHistoryIdOffset idOffset) {
        if (idOffset.getId() == 0) {
            idOffset.setProject(project);
            idOffset.setCreateTime(System.currentTimeMillis());
            idOffset.setUpdateTime(idOffset.getCreateTime());
            jdbcIdOffsetStore.save(idOffset);
        } else {
            idOffset.setUpdateTime(System.currentTimeMillis());
            jdbcIdOffsetStore.update(idOffset);
        }
    }

    public void updateWithoutMvccCheck(QueryHistoryIdOffset idOffset) {
        QueryHistoryIdOffset offset = jdbcIdOffsetStore.queryByProject(this.project, idOffset.getType());
        if (offset == null) {
            idOffset.setProject(project);
            idOffset.setCreateTime(System.currentTimeMillis());
            idOffset.setUpdateTime(idOffset.getCreateTime());
            jdbcIdOffsetStore.save(idOffset);
        } else if (idOffset.getOffset() != offset.getOffset()) {
            idOffset.setUpdateTime(System.currentTimeMillis());
            jdbcIdOffsetStore.updateWithoutCheckMvcc(idOffset);
        }
    }

    public QueryHistoryIdOffset copyForWrite(QueryHistoryIdOffset idOffset) {
        // No need to copy, just return the origin object
        // This will be rewritten after metadata is refactored
        return idOffset;
    }

    public void updateOffset(QueryHistoryIdOffset.OffsetType type, QueryHistoryIdOffsetUpdater updater) {
        QueryHistoryIdOffset cached = get(type);
        QueryHistoryIdOffset copy = copyForWrite(cached);
        updater.modify(copy);
        saveOrUpdate(copy);
    }

    public interface QueryHistoryIdOffsetUpdater {
        void modify(QueryHistoryIdOffset offset);
    }

    public QueryHistoryIdOffset get(QueryHistoryIdOffset.OffsetType type) {
        QueryHistoryIdOffset offset = jdbcIdOffsetStore.queryByProject(this.project, type.getName());
        if (offset == null) {
            return new QueryHistoryIdOffset(0, type);
        }
        return offset;
    }

    public void delete() {
        jdbcIdOffsetStore.deleteByProject(project);
    }
}

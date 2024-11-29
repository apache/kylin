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

package org.apache.kylin.metadata.table;

import static java.util.stream.Collectors.groupingBy;
import static org.apache.kylin.common.persistence.RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE;
import static org.apache.kylin.common.persistence.RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RawResourceFilter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

public class InternalTableManager {
    public static InternalTableManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, InternalTableManager.class);
    }

    static InternalTableManager newInstance(KylinConfig config, String project) {
        return new InternalTableManager(config, project);
    }

    private CachedCrudAssist<InternalTableDesc> internalTableCrud;

    private final KylinConfig config;
    private final String project;

    private ResourceStore resourceStore;

    private InternalTableManager(KylinConfig config, String project) {
        this.config = config;
        this.project = project;
        this.resourceStore = ResourceStore.getKylinMetaStore(this.config);
        initInternalTableCrud();
    }

    private void initInternalTableCrud() {
        this.internalTableCrud = new CachedCrudAssist<InternalTableDesc>(resourceStore, MetadataType.INTERNAL_TABLE,
                project, InternalTableDesc.class) {
            @Override
            protected InternalTableDesc initEntityAfterReload(InternalTableDesc entity, String resourceName) {
                entity.init(project);
                return entity;
            }
        };
    }

    public List<InternalTableDesc> listAllTables() {
        return internalTableCrud.listAll();
    }

    public Map<String, List<InternalTableDesc>> listTablesGroupBySchema() {
        return listAllTables().stream().collect(groupingBy(InternalTableDesc::getDatabase));
    }

    public InternalTableDesc getInternalTableDesc(String identity) {
        if (StringUtils.isEmpty(identity)) {
            return null;
        }
        return internalTableCrud.get(InternalTableDesc.generateResourceName(project, identity));
    }

    public void saveOrUpdateInternalTable(InternalTableDesc internalTableDesc) {
        if (internalTableCrud.contains(internalTableDesc.resourceName())) {
            updateInternalTable(internalTableDesc.getIdentity(), internalTableDesc::copyPropertiesTo);
        } else {
            createInternalTable(internalTableDesc);
        }
    }

    public void updateInternalTable(String identity, InternalTableDescUpdater updater) {
        InternalTableDesc cached = getInternalTableDesc(identity);
        if (cached == null) {
            throw new IllegalArgumentException("Internal table " + identity + " not found");
        }
        InternalTableDesc copy = copyForWrite(cached);
        updater.modify(copy);
        copy.init(project);
        internalTableCrud.save(copy);
    }

    public void removeInternalTable(String tableIdentity) {
        InternalTableDesc t = getInternalTableDesc(tableIdentity);
        if (t == null)
            return;
        internalTableCrud.delete(t);
    }

    public void createInternalTable(InternalTableDesc table) {
        table.init(project);
        InternalTableDesc copy = copyForWrite(table);
        internalTableCrud.save(copy);
    }

    public InternalTableDesc copyForWrite(InternalTableDesc table) {
        if (table.getProject() == null) {
            table.setProject(project);
        }
        return internalTableCrud.copyForWrite(table);
    }

    public void invalidCache(String resourceName) {
        internalTableCrud.invalidateCache(resourceName);
    }

    public interface InternalTableDescUpdater {
        void modify(InternalTableDesc copyForWrite);
    }

    public List<InternalTableDesc> getInternalTableDescInfos(String databaseName, String tableName, boolean isFuzzy) {
        RawResourceFilter combinedFilter = new RawResourceFilter();
        if (!StringUtils.isBlank(databaseName)) {
            if (isFuzzy) {
                combinedFilter.addConditions("dbName", Collections.singletonList(databaseName), LIKE_CASE_INSENSITIVE);
            } else {
                combinedFilter.addConditions("dbName", Collections.singletonList(databaseName), EQUAL_CASE_INSENSITIVE);
            }
        }
        if (!StringUtils.isBlank(tableName)) {
            if (isFuzzy) {
                combinedFilter.addConditions("name", Collections.singletonList(tableName), LIKE_CASE_INSENSITIVE);
            } else {
                combinedFilter.addConditions("name", Collections.singletonList(tableName), EQUAL_CASE_INSENSITIVE);
            }
        }
        return internalTableCrud.listByFilter(combinedFilter).stream().distinct().collect(Collectors.toList());
    }
}

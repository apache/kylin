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
package org.apache.kylin.tool;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KylinTableCCCleanup {

    private static final Logger logger = LoggerFactory.getLogger(KylinTableCCCleanup.class);

    private KylinConfig config;

    private boolean cleanup;

    private List<String> projects;

    public KylinTableCCCleanup(KylinConfig config, boolean cleanup, List<String> projects) {
        this.config = config;
        this.cleanup = cleanup;
        this.projects = projects;
    }

    public void scanAllTableCC() {
        for (String projectName : projects) {
            scanTableCCOfProject(projectName);
        }
    }

    private void scanTableCCOfProject(String projectName) {
        logger.info("check project {}", projectName);
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, projectName);
        Map<String, TableDesc> projectTableMap = tableMetadataManager.getAllTablesMap();
        Map<String, Set<ColumnDesc>> tableComputedColumns = Maps.newHashMap();
        for (Map.Entry<String, TableDesc> tableDescEntry : projectTableMap.entrySet()) {
            String tableIdentity = tableDescEntry.getKey();
            TableDesc tableDesc = tableDescEntry.getValue();
            Set<ColumnDesc> computedColumnDescSet = getComputedColumnDescOfTable(tableDesc);
            if (!computedColumnDescSet.isEmpty()) {
                logger.info("check table: {}", tableIdentity);
                tableComputedColumns.put(tableIdentity, computedColumnDescSet);
                processTableHasCCInMetadata(projectName, tableDesc, computedColumnDescSet);
            }
        }

        if (cleanup) {
            logger.info("project {} cleanup finished successfully.", projectName);
        }
    }

    private void processTableHasCCInMetadata(String projectName, TableDesc tableDesc,
            Set<ColumnDesc> computedColumnDescSet) {
        Set<String> ccNames = Sets.newHashSet();
        for (ColumnDesc columnDesc : computedColumnDescSet) {
            ccNames.add(columnDesc.getName());
        }
        logger.info("project {} found computed columns in table metadata: {}", projectName, ccNames);
        if (cleanup) {
            cleanupCCInTableMetadata(projectName, tableDesc);
            logger.info("project {} table cleanup finished successfully.", projectName);
        }
    }

    private void cleanupCCInTableMetadata(String project, TableDesc tableDesc) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            TableDesc newTableDesc = filterCCBeforeSave(tableDesc);
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project) //
                    .updateTableDesc(newTableDesc);
            return 0;
        }, project);

    }

    private TableDesc filterCCBeforeSave(TableDesc srcTable) {
        TableDesc copy = new TableDesc(srcTable);
        Set<ColumnDesc> newCols = Sets.newHashSet();
        ColumnDesc[] cols = copy.getColumns();
        if (ArrayUtils.isEmpty(cols)) {
            return srcTable;
        }
        for (ColumnDesc col : cols) {
            if (!col.isComputedColumn()) {
                newCols.add(col);
            }
        }
        if (newCols.size() == cols.length) {
            return srcTable;
        } else {
            copy.setColumns(newCols.toArray(new ColumnDesc[0]));
            return copy;
        }
    }

    private Set<ColumnDesc> getComputedColumnDescOfTable(TableDesc tableDesc) {
        ColumnDesc[] columnDescArray = tableDesc.getColumns();
        if (null == columnDescArray) {
            return Sets.newHashSet();
        }
        Set<ColumnDesc> computedColumnDescSet = Sets.newHashSet();
        for (ColumnDesc columnDesc : columnDescArray) {
            if (columnDesc.isComputedColumn()) {
                computedColumnDescSet.add(columnDesc);
            }
        }
        return computedColumnDescSet;
    }
}

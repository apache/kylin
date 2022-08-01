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

package org.apache.kylin.tool.util;

import static org.apache.kylin.engine.spark.utils.HiveTransactionTableHelper.generateDropTableStatement;
import static org.apache.kylin.engine.spark.utils.HiveTransactionTableHelper.generateHiveInitStatements;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.hive.HiveCmdBuilder;
import org.apache.kylin.tool.garbage.StorageCleaner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectTemporaryTableCleanerHelper {
    private final String TRANSACTIONAL_TABLE_NAME_SUFFIX = "_hive_tx_intermediate";

    public Map<String, List<String>> collectDropDBTemporaryTableNameMap(ISourceMetadataExplorer explr,
            List<StorageCleaner.FileTreeNode> jobTmps, Set<String> discardJobs) throws Exception {
        Map<String, List<String>> dropDbTableNameMap = Maps.newConcurrentMap();
        explr.listDatabases().forEach(dbName -> {
            try {
                explr.listTables(dbName).stream()
                        .filter(tableName -> tableName.contains(TRANSACTIONAL_TABLE_NAME_SUFFIX))
                        .filter(tableName -> isMatchesTemporaryTables(jobTmps, discardJobs, tableName))
                        .forEach(tableName -> putTableNameToDropDbTableNameMap(dropDbTableNameMap, dbName, tableName));
            } catch (Exception exception) {
                log.error("Failed to get the table name in the data, database: " + dbName, exception);
            }
        });
        return dropDbTableNameMap;
    }

    public boolean isMatchesTemporaryTables(List<StorageCleaner.FileTreeNode> jobTemps, Set<String> discardJobs,
            String tableName) {
        val suffixId = tableName.split(TRANSACTIONAL_TABLE_NAME_SUFFIX).length == 2
                ? tableName.split(TRANSACTIONAL_TABLE_NAME_SUFFIX)[1]
                : tableName;
        val discardJobMatch = discardJobs.stream().anyMatch(jobId -> jobId.endsWith(suffixId));
        val jobTempMatch = jobTemps.stream().anyMatch(node -> node.getRelativePath().endsWith(suffixId));
        return discardJobMatch || jobTempMatch;
    }

    public void putTableNameToDropDbTableNameMap(Map<String, List<String>> dropTableDbNameMap, String dbName,
            String tableName) {
        List<String> tableNameList = dropTableDbNameMap.containsKey(dbName) ? dropTableDbNameMap.get(dbName)
                : Lists.newArrayList();
        tableNameList.add(tableName);
        dropTableDbNameMap.put(dbName, tableNameList);
    }

    public boolean isNeedClean(boolean isEmptyJobTmp, boolean isEmptyDiscardJob) {
        return !isEmptyJobTmp || !isEmptyDiscardJob;
    }

    public String collectDropDBTemporaryTableCmd(KylinConfig kylinConfig, ISourceMetadataExplorer explr,
            List<StorageCleaner.FileTreeNode> jobTemps, Set<String> discardJobs) throws Exception {
        Map<String, List<String>> dropDbTableNameMap = collectDropDBTemporaryTableNameMap(explr, jobTemps, discardJobs);
        log.info("List of tables that meet the conditions:{}", dropDbTableNameMap);
        if (!dropDbTableNameMap.isEmpty()) {
            final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(kylinConfig);
            dropDbTableNameMap.forEach((dbName, tableNameList) -> tableNameList.forEach(tableName -> hiveCmdBuilder
                    .addStatement(generateHiveInitStatements(dbName) + generateDropTableStatement(tableName))));
            return hiveCmdBuilder.toString();
        }
        return "";
    }
}

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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.hive.HiveCmdBuilder;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectTemporaryTableCleanerHelper {
    private static final String TRANSACTIONAL_TABLE_NAME_SUFFIX = "_hive_tx_intermediate";

    public Map<String, List<String>> getDropTmpTableMap(String project, Set<String> tempTables) {
        Map<String, List<String>> dropDbTableNameMap = Maps.newConcurrentMap();
        NTableMetadataManager manager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        tempTables.forEach(tempTableName -> {
            String tableIdentity = tempTableName.split(TRANSACTIONAL_TABLE_NAME_SUFFIX).length == 2
                    ? tempTableName.split(TRANSACTIONAL_TABLE_NAME_SUFFIX)[0]
                    : tempTableName;
            TableDesc tableDesc = manager.getTableDesc(tableIdentity);
            if (Objects.nonNull(tableDesc)) {
                tempTableName = StringUtils.substringAfter(tempTableName, tableDesc.getDatabase() + ".");
                putTableToMap(dropDbTableNameMap, tableDesc.getDatabase(), tempTableName);
            } else {
                String dbName = StringUtils.substringBefore(tempTableName, ".");
                tempTableName = StringUtils.substringAfter(tempTableName, ".");
                putTableToMap(dropDbTableNameMap, dbName, tempTableName);
            }
        });
        return dropDbTableNameMap;
    }

    public void putTableToMap(Map<String, List<String>> dbTableMap, String dbName, String tableName) {
        List<String> tableNameList = dbTableMap.getOrDefault(dbName, Lists.newArrayList());
        tableNameList.add(tableName);
        dbTableMap.put(dbName, tableNameList);
    }

    public Set<String> getJobTransactionalTable(String project, String jobId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String dir = config.getJobTmpTransactionalTableDir(project, jobId);
        Path path = new Path(dir);
        Set<String> tmpTableSet = Sets.newHashSet();
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (!fs.exists(path)) {
                return tmpTableSet;
            }
            for (FileStatus fileStatus : fs.listStatus(path)) {
                String tempTableName = fileStatus.getPath().getName();
                if (StringUtils.containsIgnoreCase(tempTableName, TRANSACTIONAL_TABLE_NAME_SUFFIX)) {
                    tmpTableSet.add(tempTableName);
                }
            }
        } catch (IOException e) {
            log.error("Get Job Transactional Table failed!", e);
        }
        return tmpTableSet;
    }

    public String getDropTmpTableCmd(String project, Set<String> tempTables) {
        Map<String, List<String>> dropDbTableNameMap = getDropTmpTableMap(project, tempTables);
        if (dropDbTableNameMap.isEmpty()) {
            log.info("The temporary transaction table that will be deleted is empty.");
            return "";
        }
        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(KylinConfig.getInstanceFromEnv());
        dropDbTableNameMap.forEach((dbName, tableNameList) -> tableNameList.forEach(tableName -> {
            log.info("Temporary transaction table that will be deleted: {}.{}", dbName, tableName);
            hiveCmdBuilder.addStatement(generateHiveInitStatements(dbName) + generateDropTableStatement(tableName));
        }));
        return hiveCmdBuilder.toString();
    }
}

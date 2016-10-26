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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

/**
 * Management class to sync hive table metadata with command See main method for
 * how to use the class
 *
 * @author jianliu
 */
public class HiveSourceTableLoader {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HiveSourceTableLoader.class);

    public static Set<String> reloadHiveTables(String[] hiveTables, KylinConfig config) throws IOException {

        SetMultimap<String, String> db2tables = LinkedHashMultimap.create();
        for (String fullTableName : hiveTables) {
            String[] parts = HadoopUtil.parseHiveTableName(fullTableName);
            db2tables.put(parts[0], parts[1]);
        }

        IHiveClient hiveClient = HiveClientFactory.getHiveClient();
        SchemaChecker checker = new SchemaChecker(hiveClient, MetadataManager.getInstance(config), CubeManager.getInstance(config));
        for (Map.Entry<String, String> entry : db2tables.entries()) {
            SchemaChecker.CheckResult result = checker.allowReload(entry.getKey(), entry.getValue());
            result.raiseExceptionWhenInvalid();
        }

        // extract from hive
        Set<String> loadedTables = Sets.newHashSet();
        for (String database : db2tables.keySet()) {
            List<String> loaded = extractHiveTables(database, db2tables.get(database), hiveClient);
            loadedTables.addAll(loaded);
        }

        return loadedTables;
    }

    public static void unLoadHiveTable(String hiveTable) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        metaMgr.removeSourceTable(hiveTable);
        metaMgr.removeTableExd(hiveTable);
    }

    private static List<String> extractHiveTables(String database, Set<String> tables, IHiveClient hiveClient) throws IOException {

        List<String> loadedTables = Lists.newArrayList();
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (String tableName : tables) {
            HiveTableMeta hiveTableMeta;
            try {
                hiveTableMeta = hiveClient.getHiveTableMeta(database, tableName);
            } catch (Exception e) {
                throw new RuntimeException("cannot get HiveTableMeta", e);
            }

            TableDesc tableDesc = metaMgr.getTableDesc(database + "." + tableName);
            if (tableDesc == null) {
                tableDesc = new TableDesc();
                tableDesc.setDatabase(database.toUpperCase());
                tableDesc.setName(tableName.toUpperCase());
                tableDesc.setUuid(UUID.randomUUID().toString());
                tableDesc.setLastModified(0);
            }
            if (hiveTableMeta.tableType != null) {
                tableDesc.setTableType(hiveTableMeta.tableType);
            }

            int columnNumber = hiveTableMeta.allColumns.size();
            List<ColumnDesc> columns = new ArrayList<ColumnDesc>(columnNumber);
            for (int i = 0; i < columnNumber; i++) {
                HiveTableMeta.HiveTableColumnMeta field = hiveTableMeta.allColumns.get(i);
                ColumnDesc cdesc = new ColumnDesc();
                cdesc.setName(field.name.toUpperCase());
                // use "double" in kylin for "float"
                if ("float".equalsIgnoreCase(field.dataType)) {
                    cdesc.setDatatype("double");
                } else {
                    cdesc.setDatatype(field.dataType);
                }
                cdesc.setId(String.valueOf(i + 1));
                cdesc.setComment(field.comment);
                columns.add(cdesc);
            }
            tableDesc.setColumns(columns.toArray(new ColumnDesc[columnNumber]));

            StringBuffer partitionColumnString = new StringBuffer();
            for (int i = 0, n = hiveTableMeta.partitionColumns.size(); i < n; i++) {
                if (i > 0)
                    partitionColumnString.append(", ");
                partitionColumnString.append(hiveTableMeta.partitionColumns.get(i).name.toUpperCase());
            }

            Map<String, String> map = metaMgr.getTableDescExd(tableDesc.getIdentity());

            if (map == null) {
                map = Maps.newHashMap();
            }
            map.put(MetadataConstants.TABLE_EXD_TABLENAME, hiveTableMeta.tableName);
            map.put(MetadataConstants.TABLE_EXD_LOCATION, hiveTableMeta.sdLocation);
            map.put(MetadataConstants.TABLE_EXD_IF, hiveTableMeta.sdInputFormat);
            map.put(MetadataConstants.TABLE_EXD_OF, hiveTableMeta.sdOutputFormat);
            map.put(MetadataConstants.TABLE_EXD_OWNER, hiveTableMeta.owner);
            map.put(MetadataConstants.TABLE_EXD_LAT, String.valueOf(hiveTableMeta.lastAccessTime));
            map.put(MetadataConstants.TABLE_EXD_PC, partitionColumnString.toString());
            map.put(MetadataConstants.TABLE_EXD_TFS, String.valueOf(hiveTableMeta.fileSize));
            map.put(MetadataConstants.TABLE_EXD_TNF, String.valueOf(hiveTableMeta.fileNum));
            map.put(MetadataConstants.TABLE_EXD_PARTITIONED, Boolean.valueOf(hiveTableMeta.partitionColumns.size() > 0).toString());

            metaMgr.saveSourceTable(tableDesc);
            metaMgr.saveTableExd(tableDesc.getIdentity(), map);
            loadedTables.add(tableDesc.getIdentity());
        }

        return loadedTables;
    }

}

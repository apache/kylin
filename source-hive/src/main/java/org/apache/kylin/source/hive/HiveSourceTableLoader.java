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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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

    public static final String OUTPUT_SURFIX = "json";
    public static final String TABLE_FOLDER_NAME = "table";
    public static final String TABLE_EXD_FOLDER_NAME = "table_exd";

    public static Set<String> reloadHiveTables(String[] hiveTables, KylinConfig config) throws IOException {

        Map<String, Set<String>> db2tables = Maps.newHashMap();
        for (String table : hiveTables) {
            String[] parts = HadoopUtil.parseHiveTableName(table);
            Set<String> set = db2tables.get(parts[0]);
            if (set == null) {
                set = Sets.newHashSet();
                db2tables.put(parts[0], set);
            }
            set.add(parts[1]);
        }

        // extract from hive
        Set<String> loadedTables = Sets.newHashSet();
        for (String database : db2tables.keySet()) {
            List<String> loaded = extractHiveTables(database, db2tables.get(database), config);
            loadedTables.addAll(loaded);
        }

        return loadedTables;
    }

    public static void unLoadHiveTable(String hiveTable) throws IOException {
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        metaMgr.removeSourceTable(hiveTable);
        metaMgr.removeTableExd(hiveTable);
    }

    private static List<String> extractHiveTables(String database, Set<String> tables, KylinConfig config) throws IOException {

        List<String> loadedTables = Lists.newArrayList();
        MetadataManager metaMgr = MetadataManager.getInstance(KylinConfig.getInstanceFromEnv());
        for (String tableName : tables) {
            Table table = null;
            HiveClient hiveClient = new HiveClient();
            List<FieldSchema> partitionFields = null;
            List<FieldSchema> fields = null;
            try {
                table = hiveClient.getHiveTable(database, tableName);
                partitionFields = table.getPartitionKeys();
                fields = hiveClient.getHiveTableFields(database, tableName);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException(e);
            }

            if (fields != null && partitionFields != null && partitionFields.size() > 0) {
                fields.addAll(partitionFields);
            }

            long tableSize = hiveClient.getFileSizeForTable(table);
            long tableFileNum = hiveClient.getFileNumberForTable(table);
            TableDesc tableDesc = metaMgr.getTableDesc(database + "." + tableName);
            if (tableDesc == null) {
                tableDesc = new TableDesc();
                tableDesc.setDatabase(database.toUpperCase());
                tableDesc.setName(tableName.toUpperCase());
                tableDesc.setUuid(UUID.randomUUID().toString());
                tableDesc.setLastModified(0);
            }
            if (table.getTableType() != null) {
                tableDesc.setTableType(table.getTableType());
            }

            int columnNumber = fields.size();
            List<ColumnDesc> columns = new ArrayList<ColumnDesc>(columnNumber);
            for (int i = 0; i < columnNumber; i++) {
                FieldSchema field = fields.get(i);
                ColumnDesc cdesc = new ColumnDesc();
                cdesc.setName(field.getName().toUpperCase());
                // use "double" in kylin for "float"
                if ("float".equalsIgnoreCase(field.getType())) {
                    cdesc.setDatatype("double");
                } else {
                    cdesc.setDatatype(field.getType());
                }
                cdesc.setId(String.valueOf(i + 1));
                columns.add(cdesc);
            }
            tableDesc.setColumns(columns.toArray(new ColumnDesc[columnNumber]));

            StringBuffer partitionColumnString = new StringBuffer();
            for (int i = 0, n = partitionFields.size(); i < n; i++) {
                if (i > 0)
                    partitionColumnString.append(", ");
                partitionColumnString.append(partitionFields.get(i).getName().toUpperCase());
            }

            Map<String, String> map = metaMgr.getTableDescExd(tableDesc.getIdentity());

            if (map == null) {
                map = Maps.newHashMap();
            }
            map.put(MetadataConstants.TABLE_EXD_TABLENAME, table.getTableName());
            map.put(MetadataConstants.TABLE_EXD_LOCATION, table.getSd().getLocation());
            map.put(MetadataConstants.TABLE_EXD_IF, table.getSd().getInputFormat());
            map.put(MetadataConstants.TABLE_EXD_OF, table.getSd().getOutputFormat());
            map.put(MetadataConstants.TABLE_EXD_OWNER, table.getOwner());
            map.put(MetadataConstants.TABLE_EXD_LAT, String.valueOf(table.getLastAccessTime()));
            map.put(MetadataConstants.TABLE_EXD_PC, partitionColumnString.toString());
            map.put(MetadataConstants.TABLE_EXD_TFS, String.valueOf(tableSize));
            map.put(MetadataConstants.TABLE_EXD_TNF, String.valueOf(tableFileNum));
            map.put(MetadataConstants.TABLE_EXD_PARTITIONED, Boolean.valueOf(partitionFields != null && partitionFields.size() > 0).toString());

            metaMgr.saveSourceTable(tableDesc);
            metaMgr.saveTableExd(tableDesc.getIdentity(), map);
            loadedTables.add(tableDesc.getIdentity());
        }

        return loadedTables;
    }

}

package com.kylinolap.metadata.tool;

/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.ResourceTool;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;

/**
 * Management class to sync hive table metadata with command See main method for
 * how to use the class
 * 
 * @author jianliu
 */
public class HiveSourceTableLoader {

    private static final Logger logger = LoggerFactory.getLogger(HiveSourceTableLoader.class);

    public static final String OUTPUT_SURFIX = "json";
    public static final String TABLE_FOLDER_NAME = "table";
    public static final String TABLE_EXD_FOLDER_NAME = "table_exd";

    protected static HiveMetaStoreClient client = HiveClient.getInstance().getMetaStoreClient();

    public static Set<String> reloadHiveTables(String[] hiveTables, KylinConfig config) throws IOException {
        
        Map<String, Set<String>> db2tables = Maps.newHashMap();
        for (String table : hiveTables) {
            int cut = table.indexOf('.');
            String database = cut >= 0 ? table.substring(0, cut).trim() : "DEFAULT";
            String tableName = cut >= 0 ? table.substring(cut + 1).trim() : table.trim();
            Set<String> set = db2tables.get(database);
            if (set == null) {
                set = Sets.newHashSet();
                db2tables.put(database, set);
            }
            set.add(tableName);
        }

        // metadata tmp dir
        File metaTmpDir = File.createTempFile("meta_tmp", null);
        metaTmpDir.delete();
        metaTmpDir.mkdirs();

        for (String database : db2tables.keySet()) {
            for (String table : db2tables.get(database)) {
                TableDesc tableDesc = MetadataManager.getInstance(config).getTableDesc(table);
                if (tableDesc == null) {
                    continue;
                }
                if (tableDesc.getDatabase().equalsIgnoreCase(database)) {
                    continue;
                } else {
                    throw new UnsupportedOperationException(String.format("there is already a table[%s] in database[%s]", tableDesc.getName(), tableDesc.getDatabase()));
                }
            }
        }

        // extract from hive
        Set<String> loadedTables = Sets.newHashSet();
        for (String database : db2tables.keySet()) {
            List<String> loaded = extractHiveTables(database, db2tables.get(database), metaTmpDir, config);
            loadedTables.addAll(loaded);
        }

        // save loaded tables
        ResourceTool.copy(KylinConfig.createInstanceFromUri(metaTmpDir.getAbsolutePath()), config);

        return loadedTables;
    }

    private static List<String> extractHiveTables(String database, Set<String> tables, File metaTmpDir, KylinConfig config) throws IOException {
        File tableDescDir = new File(metaTmpDir, TABLE_FOLDER_NAME);
        File tableExdDir = new File(metaTmpDir, TABLE_EXD_FOLDER_NAME);
        mkdirs(tableDescDir);
        mkdirs(tableExdDir);

        List<TableDesc> tableDescList = new ArrayList<TableDesc>();
        List<Map<String, String>> tableAttrsList = new ArrayList<Map<String, String>>();
        
        for (String tableName : tables) {
            Table table = null;
            List<FieldSchema> fields = null;
            try {
                table = client.getTable(database, tableName);
                fields = client.getFields(database, tableName);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                throw new IOException(e);
            } 

            TableDesc tableDesc = new TableDesc();
            tableDesc.setDatabase(database.toUpperCase());
            tableDesc.setName(tableName.toUpperCase());
            tableDesc.setUuid(UUID.randomUUID().toString());
            int columnNumber = fields.size();
            List<ColumnDesc> columns = new ArrayList<ColumnDesc>(columnNumber);
            for (int i = 0; i < columnNumber; i++) {
                FieldSchema field = fields.get(i);
                ColumnDesc cdesc = new ColumnDesc();
                cdesc.setName(field.getName().toUpperCase());
                cdesc.setDatatype(field.getType());
                cdesc.setId(String.valueOf(i + 1));
                columns.add(cdesc);
            }
            tableDesc.setColumns(columns.toArray(new ColumnDesc[columnNumber]));
            
            List<FieldSchema> partitionCols = table.getPartitionKeys();
            StringBuffer partitionColumnString = new StringBuffer();
            for(int i=0, n= partitionCols.size(); i<n; i++) {
                if(i >0)
                    partitionColumnString.append(", ");
                partitionColumnString.append(partitionCols.get(i).getName().toUpperCase());
            }
            tableDescList.add(tableDesc);
            Map<String, String> map =  new HashMap<String, String>(); //table.getParameters();
            map.put("tableName", table.getTableName());
            map.put("location", table.getSd().getLocation());
            map.put("inputformat", table.getSd().getInputFormat());
            map.put("outputformat", table.getSd().getOutputFormat());
            map.put("owner", table.getOwner());
            map.put("lastAccessTime", String.valueOf(table.getLastAccessTime()));
            map.put("partitionColumns", partitionColumnString.toString());
            tableAttrsList.add(map);
        }

        List<String> loadedTables = Lists.newArrayList();
        
        for (TableDesc table : tableDescList) {
            File file = new File(tableDescDir, table.getName().toUpperCase() + "." + OUTPUT_SURFIX);
            JsonUtil.writeValueIndent(new FileOutputStream(file), table);
            loadedTables.add(table.getDatabase() + "." + table.getName());
        }

        for (Map<String, String> tableAttrs : tableAttrsList) {
            File file = new File(tableExdDir, tableAttrs.get("tableName").toUpperCase() + "." + OUTPUT_SURFIX);
            JsonUtil.writeValueIndent(new FileOutputStream(file), tableAttrs);
        }

        return loadedTables;
    }


    private static void mkdirs(File metaTmpDir) {
        if (!metaTmpDir.exists()) {
            if (!metaTmpDir.mkdirs()) {
                throw new IllegalArgumentException("Failed to create Output dir : " + metaTmpDir.getAbsolutePath());
            }
        }
    }

    /**
     */
    public static void main(String[] args) {
    }
}

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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.kylinolap.metadata.MetadataManager;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.ResourceTool;
import com.kylinolap.common.util.CliCommandExecutor;
import com.kylinolap.common.util.JsonUtil;
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

        for (String database: db2tables.keySet()) {
            for (String table: db2tables.get(database)) {
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
        StringBuilder cmd = new StringBuilder();
        cmd.append("hive -e \"");
        if (StringUtils.isEmpty(database) == false) {
            cmd.append("use " + database + "; ");
        }
        for (String table : tables) {
            cmd.append("show table extended like " + table + "; ");
        }
        cmd.append("\"");

        CliCommandExecutor cmdExec = config.getCliCommandExecutor();
        String output = cmdExec.execute(cmd.toString());

        return extractTableDescFromHiveOutput(database, output, metaTmpDir);
    }

    private static List<String> extractTableDescFromHiveOutput(String database, String hiveOutput, File metaTmpDir) throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(hiveOutput));
        try {
            return extractTables(database, reader, metaTmpDir);
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }

    private static List<String> extractTables(String database, BufferedReader reader, File metaTmpDir) throws IOException {

        File tableDescDir = new File(metaTmpDir, TABLE_FOLDER_NAME);
        File tableExdDir = new File(metaTmpDir, TABLE_EXD_FOLDER_NAME);
        mkdirs(tableDescDir);
        mkdirs(tableExdDir);

        List<TableDesc> tableDescList = new ArrayList<TableDesc>();
        List<Map<String, String>> tableAttrsList = new ArrayList<Map<String, String>>();
        getTables(database, reader, tableDescList, tableAttrsList);

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

    private static void getTables(String database, BufferedReader reader, //
            List<TableDesc> tableDescList, List<Map<String, String>> tableAttrsList) throws IOException {

        Map<String, String> tableAttrs = new HashMap<String, String>();
        TableDesc tableDesc = new TableDesc();
        String line;
        boolean hit = false;
        
        while ((line = reader.readLine()) != null) {
            logger.info(line);
            int i = line.indexOf(":");
            if (i == -1) {
                continue;
            }
            String key = line.substring(0, i);
            String value = line.substring(i + 1, line.length());
            if (key.equals("tableName")) {// Create a new table object
                hit = true;
                tableAttrs = new HashMap<String, String>();
                tableAttrsList.add(tableAttrs);
                tableDesc = new TableDesc();
                tableDescList.add(tableDesc);
            }

            if (!hit) {
                continue;
            }

            if (line.startsWith("columns")) {// geneate source table metadata
                String tname = tableAttrs.get("tableName");

                tableDesc.setDatabase(database.toUpperCase());
                tableDesc.setName(tname.toUpperCase());
                tableDesc.setUuid(UUID.randomUUID().toString());
                addColumns(tableDesc, value);
            }
            tableAttrs.put(key, value);
            if (key.equals("lastUpdateTime")) {
                hit = false;
            }
        }

    }

    private static void addColumns(TableDesc sTable, String value) {
        List<ColumnDesc> columns = new ArrayList<ColumnDesc>();
        int i1 = value.indexOf("{");
        int i2 = value.indexOf("}");
        if (i1 < 0 || i2 < 0 || i1 > i2) {
            return;
        }
        String temp = value.substring(i1 + 1, i2);
        String[] strArr = temp.split(", ");
        for (int i = 0; i < strArr.length; i++) {
            String t1 = strArr[i].trim();
            int pos = t1.indexOf(" ");
            String colType = t1.substring(0, pos).trim();
            String colName = t1.substring(pos).trim();
            ColumnDesc cdesc = new ColumnDesc();
            cdesc.setName(colName.toUpperCase());
            cdesc.setDatatype(convertType(colType));
            cdesc.setId(String.valueOf(i + 1));
            columns.add(cdesc);
        }
        sTable.setColumns(columns.toArray(new ColumnDesc[0]));
    }

    private static String convertType(String colType) {
        if ("i32".equals(colType)) {
            return "int";
        } else if ("i64".equals(colType)) {
            return "bigint";
        } else if ("i16".equals(colType)) {
            return "smallint";
        } else if ("byte".equals(colType)) {
            return "tinyint";
        } else if ("bool".equals(colType))
            return "boolean";
        return colType;
    }

    /**
     */
    public static void main(String[] args) {
    }
}

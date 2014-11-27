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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.ResourceTool;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.TableDesc;

/**
 * Management class to sync hive table metadata with command See main method for
 * how to use the class
 * 
 * @author jianliu
 */
public class HiveSourceTableMgmt {
    private static final Logger logger = LoggerFactory.getLogger(HiveSourceTableMgmt.class);
    public static final String OUTPUT_SURFIX = "json";
    public static final String TABLE_FOLDER_NAME = "table";
    public static final String TABLE_EXD_FOLDER_NAME = "table_exd";

    public static InputStream executeHiveCommand(String command) throws IOException {

        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", "hive -e \"" + command + "\"");

        // Run hive
        pb.redirectErrorStream(true);
        Process p = pb.start();
        InputStream is = p.getInputStream();

        return is;
    }

    private void closeInputStream(InputStream is) {
        try {
            if (is != null)
                is.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    public void extractTableDescFromFile(String hiveOutputPath, String tableMetaOutcomeDir) {
        InputStream is = getHiveOutputStreamFromFile(hiveOutputPath);
        extractTables(is, tableMetaOutcomeDir);
        closeInputStream(is);

    }

    public void extractTableDescWithTablePattern(String tablePattern, String tableMetaOutcomeDir) {
        InputStream is = getHiveOutputStreamByExec(tablePattern);
        extractTables(is, tableMetaOutcomeDir);
        closeInputStream(is);
    }

    private void extractTables(InputStream is, String tableMetaOutcomeDir) {

        List<TableDesc> tableDescList = new ArrayList<TableDesc>();
        List<Map<String, String>> tableAttrsList = new ArrayList<Map<String, String>>();
        getTables(is, tableDescList, tableAttrsList);
        File dir = new File(tableMetaOutcomeDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IllegalArgumentException("Failed to create Output dir : " + dir.getAbsolutePath());
            }
        }
        File tableDescDir = new File(dir, TABLE_FOLDER_NAME);
        File tableExdDir = new File(dir, TABLE_EXD_FOLDER_NAME);
        if (!tableDescDir.exists()) {
            if (!tableDescDir.mkdirs()) {
                throw new IllegalArgumentException("Failed to create Output dir : " + tableDescDir.getAbsolutePath());
            }
        }
        if (!tableExdDir.exists()) {
            if (!tableExdDir.mkdirs()) {
                throw new IllegalArgumentException("Failed to create Output dir : " + tableExdDir.getAbsolutePath());
            }
        }

        for (TableDesc table : tableDescList) {
            File file = new File(tableDescDir, table.getName().toUpperCase() + "." + OUTPUT_SURFIX);
            try {
                JsonUtil.writeValueIndent(new FileOutputStream(file), table);
            } catch (JsonGenerationException e) {
                throw new IllegalArgumentException(e);
            } catch (JsonMappingException e) {
                throw new IllegalArgumentException(e);
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException(e);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }

        for (Map<String, String> tableAttrs : tableAttrsList) {
            File file = new File(tableExdDir, tableAttrs.get("tableName").toUpperCase() + "." + OUTPUT_SURFIX);
            try {
                JsonUtil.writeValueIndent(new FileOutputStream(file), tableAttrs);
            } catch (JsonGenerationException e) {
                throw new IllegalArgumentException(e);
            } catch (JsonMappingException e) {
                throw new IllegalArgumentException(e);
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException(e);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public InputStream getHiveOutputStreamFromFile(String hiveOutputPath) {
        try {
            return new FileInputStream(new File(hiveOutputPath));
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    public InputStream getHiveOutputStreamByExec(String tablePattern) {
        try {
            String query = "show table extended like \\`" + tablePattern + "\\`";
            logger.info("Running hive query: " + query);
            return executeHiveCommand(query);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    public void getTables(InputStream is, List<TableDesc> tableDescList, List<Map<String, String>> tableAttrsList) {

        InputStreamReader reader = new InputStreamReader(is);
        BufferedReader bufferedReader = new BufferedReader(reader);
        Map<String, String> tableAttrs = new HashMap<String, String>();
        TableDesc tableDesc = new TableDesc();
        // mTableList.add(mTable);
        // sTableList.add(sTable);
        String line;
        boolean hit = false;
        try {
            line = bufferedReader.readLine();
            while (line != null) {
                logger.info(line);
                int i = line.indexOf(":");
                if (i == -1) {
                    // System.out.println("Wrong line: " + line);
                    line = bufferedReader.readLine();
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
                    line = bufferedReader.readLine();
                    continue;
                }

                if (line.startsWith("columns")) {// geneate source table
                    // metadata
                    String tname = tableAttrs.get("tableName");

                    // TODO, should not assume database to be "default"
                    tableDesc.setDatabase("default".toUpperCase());
                    tableDesc.setName(tname.toUpperCase());
                    tableDesc.setUuid(UUID.randomUUID().toString());
                    addColumns(tableDesc, value);
                }
                tableAttrs.put(key, value);
                if (key.equals("lastUpdateTime")) {
                    hit = false;
                }
                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

    }

    private void addColumns(TableDesc sTable, String value) {
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

    private String convertType(String colType) {
        if ("i32".equals(colType)) {
            return "int";
        } else if ("i64".equals(colType)) {
            return "bigint";
        } else if ("i16".equals(colType)) {
            return "smallint";
        } else if ("byte".equals(colType)) {
            return "tinyint";
        }
        return colType;
    }

    /**
     * @param tables
     */
    public static String reloadHiveTable(String tables) {
        // Rewrite the table string
        String[] tokens = StringUtils.split(tables, ",");
        StringBuffer buff = new StringBuffer();
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i].trim();
            if (StringUtils.isNotEmpty(token)) {
                buff.append(";");
                buff.append("show table extended like " + token);
            }
        }
        String hiveCommand = buff.toString();
        if (StringUtils.isEmpty(hiveCommand)) {
            throw new IllegalArgumentException("The required tabes is empty");
        }
        // Call shell to generate source tabe
        String tableMetaDir = "";
        try {
            tableMetaDir = callGenerateCommand(hiveCommand);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Failed to get metadata from upstream, for tables " + tables, e);
        }
        // Update Metadata
        if (StringUtils.isEmpty(tableMetaDir)) {
            throw new IllegalArgumentException("Failed to get metadata from upstream, for tables " + tables);
        }
        File dir = new File(tableMetaDir);
        if (!dir.exists() || !dir.isDirectory() || dir.list().length == 0) {
            throw new IllegalArgumentException("Nothing found from path " + tableMetaDir);
        }

        copyToMetaTable(tableMetaDir);

        return tableMetaDir;
    }

    /**
     * @throws IOException
     */
    private static void copyToMetaTable(String metaPath) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        try {
            ResourceTool.copy(KylinConfig.createInstanceFromUri(metaPath), config);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Failed to copy resources", e);
        }
        logger.info("Successfully Copied metadata from " + metaPath);

    }

    /**
     * @param hiveCommd
     */
    private static String callGenerateCommand(String hiveCommd) throws IOException {
        // Get out put path
        String tempDir = System.getProperty("java.io.tmpdir");
        logger.info("OS current temporary directory is " + tempDir);
        if (StringUtils.isEmpty(tempDir)) {
            tempDir = "/tmp";
        }
        String[] cmd = new String[2];
        String osName = System.getProperty("os.name");
        if (osName.startsWith("Windows")) {
            cmd[0] = "cmd.exe";
            cmd[1] = "/C";
        } else {
            cmd[0] = "/bin/bash";
            cmd[1] = "-c";
        }

        // hive command output
        // String hiveOutputPath = tempDir + File.separator +
        // "tmp_kylin_output";
        String hiveOutputPath = File.createTempFile("HiveOutput", null).getAbsolutePath();
        // Metadata output
        File dir = File.createTempFile("meta", null);
        dir.delete();
        dir.mkdir();
        String tableMetaOutcomeDir = dir.getAbsolutePath();

        ProcessBuilder pb = null;

        if (osName.startsWith("Windows")) {
            pb = new ProcessBuilder(cmd[0], cmd[1], "ssh root@sandbox 'hive -e \"" + hiveCommd + "\"' > " + hiveOutputPath);
        } else {
            pb = new ProcessBuilder(cmd[0], cmd[1], "hive -e \"" + hiveCommd + "\" > " + hiveOutputPath);
        }

        // Run hive
        pb.directory(new File(tempDir));
        pb.redirectErrorStream(true);
        Process p = pb.start();
        InputStream is = p.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = null;

        try {
            br = new BufferedReader(isr);
            String line = null;
            logger.info("Execute : " + pb.command().get(0));
            while ((line = br.readLine()) != null) {
                logger.info(line);
            }
        } finally {
            if (null != br) {
                br.close();
            }
        }
        logger.info("Hive execution completed!");

        HiveSourceTableMgmt rssMgmt = new HiveSourceTableMgmt();
        rssMgmt.extractTableDescFromFile(hiveOutputPath, tableMetaOutcomeDir);
        return tableMetaOutcomeDir;
    }

    /**
     * @param args
     *            HiveSourceTableMgmt jdbc:hive2://kylin-local:10000/default,
     *            hive, hive, c:\\temp [, name]
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Wrong arguments, example");
            System.out.println("Get data from command: -c Kylin*, c:\\temp");
            System.out.println("Get data from outputfile: -f /tmp/hiveout.txt, c:\\temp");
            System.exit(1);
        }
        String mode = args[0];
        String tableMetaOutcomeDir = args[2];
        HiveSourceTableMgmt rssMgmt = new HiveSourceTableMgmt();
        try {

            if (mode.equalsIgnoreCase("-c")) {
                rssMgmt.extractTableDescWithTablePattern(args[1], tableMetaOutcomeDir);
            }

            if (mode.equalsIgnoreCase("-f")) {
                rssMgmt.extractTableDescFromFile(args[1], tableMetaOutcomeDir);
            }

        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }
}

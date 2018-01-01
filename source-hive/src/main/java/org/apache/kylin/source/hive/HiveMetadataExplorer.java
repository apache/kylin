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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetadataExplorer implements ISourceMetadataExplorer, ISampleDataDeployer {

    private static final Logger logger = LoggerFactory.getLogger(HiveClientFactory.class);

    IHiveClient hiveClient = HiveClientFactory.getHiveClient();

    @Override
    public List<String> listDatabases() throws Exception {
        return hiveClient.getHiveDbNames();
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        return hiveClient.getHiveTableNames(database);
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(String database, String tableName, String prj) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        TableMetadataManager metaMgr = TableMetadataManager.getInstance(config);

        HiveTableMeta hiveTableMeta;
        try {
            hiveTableMeta = hiveClient.getHiveTableMeta(database, tableName);
        } catch (Exception e) {
            throw new RuntimeException("cannot get HiveTableMeta", e);
        }

        TableDesc tableDesc = metaMgr.getTableDesc(database + "." + tableName, prj);

        // make a new TableDesc instance, don't modify the one in use
        if (tableDesc == null) {
            tableDesc = new TableDesc();
            tableDesc.setDatabase(database.toUpperCase());
            tableDesc.setName(tableName.toUpperCase());
            tableDesc.setUuid(UUID.randomUUID().toString());
            tableDesc.setLastModified(0);
        } else {
            tableDesc = new TableDesc(tableDesc);
        }

        if (hiveTableMeta.tableType != null) {
            tableDesc.setTableType(hiveTableMeta.tableType);
        }

        tableDesc.setColumns(extractColumnFromMeta(hiveTableMeta));

        StringBuffer partitionColumnString = new StringBuffer();
        for (int i = 0, n = hiveTableMeta.partitionColumns.size(); i < n; i++) {
            if (i > 0)
                partitionColumnString.append(", ");
            partitionColumnString.append(hiveTableMeta.partitionColumns.get(i).name.toUpperCase());
        }

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setUuid(UUID.randomUUID().toString());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        tableExtDesc.addDataSourceProp("location", hiveTableMeta.sdLocation);
        tableExtDesc.addDataSourceProp("owner", hiveTableMeta.owner);
        tableExtDesc.addDataSourceProp("last_access_time", String.valueOf(hiveTableMeta.lastAccessTime));
        tableExtDesc.addDataSourceProp("partition_column", partitionColumnString.toString());
        tableExtDesc.addDataSourceProp("total_file_size", String.valueOf(hiveTableMeta.fileSize));
        tableExtDesc.addDataSourceProp("total_file_number", String.valueOf(hiveTableMeta.fileNum));
        tableExtDesc.addDataSourceProp("hive_inputFormat", hiveTableMeta.sdInputFormat);
        tableExtDesc.addDataSourceProp("hive_outputFormat", hiveTableMeta.sdOutputFormat);
        tableExtDesc.addDataSourceProp("skip_header_line_count", String.valueOf(hiveTableMeta.skipHeaderLineCount));

        return Pair.newPair(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        hiveClient.executeHQL(generateCreateSchemaSql(database));
    }

    private String generateCreateSchemaSql(String schemaName) {
        return String.format("CREATE DATABASE IF NOT EXISTS %s", schemaName);
    }

    @Override
    public void createSampleTable(TableDesc table) throws Exception {
        hiveClient.executeHQL(generateCreateTableSql(table));
    }

    private String[] generateCreateTableSql(TableDesc tableDesc) {

        String dropsql = "DROP TABLE IF EXISTS " + tableDesc.getIdentity();
        String dropsql2 = "DROP VIEW IF EXISTS " + tableDesc.getIdentity();

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE " + tableDesc.getIdentity() + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getName() + " " + getHiveDataType((col.getDatatype())) + "\n");
        }

        ddl.append(")" + "\n");
        ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + "\n");
        ddl.append("STORED AS TEXTFILE");

        return new String[] { dropsql, dropsql2, ddl.toString() };
    }

    @Override
    public void loadSampleData(String tableName, String tmpDataDir) throws Exception {
        hiveClient.executeHQL(generateLoadDataSql(tableName, tmpDataDir));
    }

    private String generateLoadDataSql(String tableName, String tableFileDir) {
        return "LOAD DATA LOCAL INPATH '" + tableFileDir + "/" + tableName + ".csv' OVERWRITE INTO TABLE " + tableName;
    }

    @Override
    public void createWrapperView(String origTableName, String viewName) throws Exception {
        hiveClient.executeHQL(generateCreateViewSql(viewName, origTableName));
    }

    private String[] generateCreateViewSql(String viewName, String tableName) {

        String dropView = "DROP VIEW IF EXISTS " + viewName;
        String dropTable = "DROP TABLE IF EXISTS " + viewName;

        String createSql = ("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        return new String[] { dropView, dropTable, createSql };
    }

    private static String getHiveDataType(String javaDataType) {
        String hiveDataType = javaDataType.toLowerCase().startsWith("varchar") ? "string" : javaDataType;
        hiveDataType = javaDataType.toLowerCase().startsWith("integer") ? "int" : hiveDataType;

        return hiveDataType.toLowerCase();
    }

    @Override
    public ColumnDesc[] evalQueryMetadata(String query) {
        if (StringUtils.isEmpty(query)) {
            throw new RuntimeException("Evalutate query shall not be empty.");
        }
        
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String tmpDatabase = config.getHiveDatabaseForIntermediateTable();
        String tmpView = "kylin_eval_query_" + UUID.nameUUIDFromBytes(query.getBytes()).toString().replace("-", "");
        
        String dropViewSql = "DROP VIEW IF EXISTS " + tmpDatabase + "." + tmpView;
        String evalViewSql = "CREATE VIEW " + tmpDatabase + "." + tmpView + " as " + query;
        
        try {
            logger.debug("Removing duplicate view {}", tmpView);
            hiveClient.executeHQL(dropViewSql);
            logger.debug("Creating view {} for query: {}", tmpView, query);
            hiveClient.executeHQL(evalViewSql);
            logger.debug("Evaluating query columns' metadata");
            HiveTableMeta hiveTableMeta = hiveClient.getHiveTableMeta(tmpDatabase, tmpView);
            return extractColumnFromMeta(hiveTableMeta);
        } catch (Exception e) {
            throw new RuntimeException("Cannot evalutate metadata of query: " + query, e);
        } finally {
            try {
                logger.debug("Cleaning up.");
                hiveClient.executeHQL(dropViewSql);
            } catch (Exception e) {
                logger.warn("Cannot drop temp view of query: {}", query, e);
            }
        }
    }

    private ColumnDesc[] extractColumnFromMeta(HiveTableMeta hiveTableMeta) {
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

        return columns.toArray(new ColumnDesc[columnNumber]);
    }
}

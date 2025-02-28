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
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetadataExplorer implements ISourceMetadataExplorer, ISampleDataDeployer {
    public static final Logger logger = LoggerFactory.getLogger(HiveMetadataExplorer.class);
    IHiveClient hiveClient = HiveClientFactory.getHiveClient();

    private static String getHiveDataType(String javaDataType) {
        String hiveDataType = javaDataType.toLowerCase(Locale.ROOT).startsWith("varchar") ? "string" : javaDataType;
        hiveDataType = javaDataType.toLowerCase(Locale.ROOT).startsWith("integer") ? "int" : hiveDataType;

        return hiveDataType.toLowerCase(Locale.ROOT);
    }

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
        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(config, prj);

        HiveTableMeta hiveTableMeta;
        try {
            hiveTableMeta = hiveClient.getHiveTableMeta(database, tableName);
        } catch (Exception e) {
            throw new RuntimeException("cannot get HiveTableMeta", e);
        }

        TableDesc tableDesc = metaMgr.getTableDesc(database + "." + tableName);

        // make a new TableDesc instance, don't modify the one in use
        if (tableDesc == null) {
            tableDesc = new TableDesc();
            tableDesc.setDatabase(database.toUpperCase(Locale.ROOT));
            tableDesc.setName(tableName.toUpperCase(Locale.ROOT));
            tableDesc.setUuid(RandomUtil.randomUUIDStr());
            tableDesc.setLastModified(0);
        } else {
            tableDesc = new TableDesc(tableDesc);
        }

        if (hiveTableMeta.tableType != null) {
            tableDesc.setTableType(hiveTableMeta.tableType);
        }

        tableDesc.setRangePartition(checkIsRangePartitionTable(hiveTableMeta.allColumns));
        int columnNumber = hiveTableMeta.allColumns.size();
        List<ColumnDesc> columns = getColumnDescs(hiveTableMeta.allColumns);
        tableDesc.setColumns(columns.toArray(new ColumnDesc[columnNumber]));

        StringBuilder partitionColumnString = new StringBuilder();
        for (int i = 0, n = hiveTableMeta.partitionColumns.size(); i < n; i++) {
            if (i > 0)
                partitionColumnString.append(", ");
            partitionColumnString.append(hiveTableMeta.partitionColumns.get(i).name.toUpperCase(Locale.ROOT));
        }

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setUuid(RandomUtil.randomUUIDStr());
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

    public List<ColumnDesc> getColumnDescs(List<HiveTableMeta.HiveTableColumnMeta> columnMetaList) {
        List<ColumnDesc> columns = new ArrayList<>(columnMetaList.size());
        Set<String> columnCacheTemp = Sets.newHashSet();
        IntStream.range(0, columnMetaList.size()).forEach(i -> {
            HiveTableMeta.HiveTableColumnMeta field = columnMetaList.get(i);
            if (columnCacheTemp.contains(field.name)) {
                logger.info("The【{}】column is already included and does not need to be added again", field.name);
                return;
            }
            columnCacheTemp.add(field.name);
            ColumnDesc cdesc = new ColumnDesc();
            cdesc.setName(field.name.toUpperCase(Locale.ROOT));
            // use "double" in kylin for "float"
            if ("float".equalsIgnoreCase(field.dataType)) {
                cdesc.setDatatype("double");
            } else {
                cdesc.setDatatype(field.dataType);
            }
            cdesc.setId(String.valueOf(i + 1));
            cdesc.setComment(field.comment);
            columns.add(cdesc);
        });
        return columns;
    }

    public boolean checkIsRangePartitionTable(List<HiveTableMeta.HiveTableColumnMeta> columnMetas) {
        return columnMetas.stream().collect(Collectors.groupingBy(p -> p.name)).values().stream()
                .anyMatch(p -> p.size() > 1);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public boolean checkDatabaseAccess(String database) throws Exception {
        return true;
    }

    @Override
    public boolean checkTablesAccess(Set<String> tables) {
        return true;
    }

    @Override
    public Set<String> getTablePartitions(String database, String table, String prj, String partitionCols) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        hiveClient.executeHQL(generateCreateSchemaSql(database));
    }

    private String generateCreateSchemaSql(String schemaName) {
        return String.format(Locale.ROOT, "CREATE DATABASE IF NOT EXISTS %s", schemaName);
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

}

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
package org.apache.kylin.engine.spark.source;

import static org.apache.kylin.common.exception.ServerErrorCode.DDL_CHECK_ERROR;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;

import lombok.val;

public class NSparkMetadataExplorer implements ISourceMetadataExplorer, ISampleDataDeployer, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkMetadataExplorer.class);

    public static String generateCreateSchemaSql(String schemaName) {
        return String.format(Locale.ROOT, "CREATE DATABASE IF NOT EXISTS %s", schemaName);
    }

    public static String[] generateCreateTableSql(TableDesc tableDesc) {
        String dropSql = "DROP TABLE IF EXISTS " + tableDesc.getIdentity();

        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE " + tableDesc.getIdentity() + "\n");
        ddl.append("(" + "\n");

        for (int i = 0; i < tableDesc.getColumns().length; i++) {
            ColumnDesc col = tableDesc.getColumns()[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(col.getName() + " " + col.getDatatype() + "\n");
        }

        ddl.append(")" + "\n");
        ddl.append("USING com.databricks.spark.csv");

        return new String[] { dropSql, ddl.toString() };
    }

    public NSparkTableMetaExplorer getTableMetaExplorer() {
        return new NSparkTableMetaExplorer();
    }

    @Override
    public List<String> listDatabases() throws Exception {
        Dataset<Row> dataset = SparderEnv.getSparkSession().sql("show databases").select("namespace");
        List<String> databases = dataset.collectAsList().stream().map(row -> row.getString(0))
                .collect(Collectors.toList());
        if (KylinConfig.getInstanceFromEnv().isDDLLogicalViewEnabled()) {
            String logicalViewDB = KylinConfig.getInstanceFromEnv().getDDLLogicalViewDB();
            databases.forEach(db -> {
                if (db.equalsIgnoreCase(logicalViewDB)) {
                    throw new KylinException(DDL_CHECK_ERROR,
                            "Logical view database should not be duplicated " + "with normal hive database!!!");
                }
            });
            List<String> databasesWithLogicalDB = Lists.newArrayList();
            databasesWithLogicalDB.add(logicalViewDB);
            databasesWithLogicalDB.addAll(databases);
            databases = databasesWithLogicalDB;
        }
        return databases;
    }

    @Override
    public List<String> listTables(String database) throws Exception {
        val ugi = UserGroupInformation.getCurrentUser();
        val config = KylinConfig.getInstanceFromEnv();
        val spark = SparderEnv.getSparkSession();

        List<String> tables = Lists.newArrayList();
        try {
            String sql = "show tables";
            if (StringUtils.isNotBlank(database)) {
                sql = String.format(Locale.ROOT, sql + " in %s", database);
            }
            Dataset<Row> dataset = SparderEnv.getSparkSession().sql(sql).select("tableName");
            tables = dataset.collectAsList().stream().map(row -> row.getString(0)).collect(Collectors.toList());

            if (config.getTableAccessFilterEnable() && config.getKerberosProjectLevelEnable()
                    && UserGroupInformation.isSecurityEnabled()) {
                List<String> accessTables = Lists.newArrayList();
                for (String table : tables) {
                    val tableName = database + "." + table;
                    if (checkTableAccess(tableName)) {
                        accessTables.add(table);
                    }
                }
                return accessTables;
            }
        } catch (Exception e) {
            logger.error("List hive tables failed. user: {}, db: {}", ugi.getUserName(), database);
        }

        return tables;
    }

    public boolean checkTableAccess(String tableName) {
        boolean isAccess = true;
        try {
            val spark = SparderEnv.getSparkSession();
            val sparkTable = spark.catalog().getTable(tableName);
            Set<String> needCheckTables = Sets.newHashSet();
            if (sparkTable.tableType().equals(CatalogTableType.VIEW().name())) {
                needCheckTables = SparkSqlUtil.getViewOrignalTables(tableName, SparderEnv.getSparkSession());
            } else {
                needCheckTables.add(tableName);
            }
            String hiveSpecFsLocation = spark.sessionState().conf().getConf(SQLConf.HIVE_SPECIFIC_FS_LOCATION());
            FileSystem fs = null == hiveSpecFsLocation ? HadoopUtil.getWorkingFileSystem()
                    : HadoopUtil.getFileSystem(hiveSpecFsLocation);
            for (String table : needCheckTables) {
                val loc = getLoc(spark, table, hiveSpecFsLocation);
                if (loc.startsWith(fs.getScheme()) || loc.startsWith("/")) {
                    fs.listStatus(new Path(loc));
                } else {
                    HadoopUtil.getFileSystem(loc).listStatus(new Path(loc));
                }
            }
        } catch (Exception e) {
            isAccess = false;
            try {
                logger.error("Read hive table {} error:{}, ugi name: {}.", tableName, e.getMessage(),
                        UserGroupInformation.getCurrentUser().getUserName());
            } catch (IOException ex) {
                logger.error("fetch user curr ugi info error.", e);
            }
        }
        return isAccess;
    }

    public boolean checkDatabaseHadoopAccessFast(String database) throws Exception {
        boolean isAccess = true;
        val spark = SparderEnv.getSparkSession();
        try {
            String databaseLocation = spark.catalog().getDatabase(database).locationUri();
            RemoteIterator<FileStatus> tablesIterator = getFilesIterator(databaseLocation, false);
            if (tablesIterator.hasNext()) {
                Path tablePath = tablesIterator.next().getPath();
                getFilesIterator(tablePath.toString(), true);
            }
        } catch (Exception e) {
            isAccess = false;
            try {
                logger.error("Read hive database {} error:{}, ugi name: {}.", database, e.getMessage(),
                        UserGroupInformation.getCurrentUser().getUserName());
            } catch (IOException ex) {
                logger.error("fetch user curr ugi info error.", e);
            }
        }
        return isAccess;
    }

    private RemoteIterator<FileStatus> getFilesIterator(String location, boolean checkList) throws IOException {
        val sparkConf = SparderEnv.getSparkSession().sessionState().conf();
        String hiveSpecFsLocation;
        FileSystem fs;
        if (sparkConf.contains("spark.sql.hive.specific.fs.location")) {
            hiveSpecFsLocation = sparkConf.getConf(SQLConf.HIVE_SPECIFIC_FS_LOCATION());
            location = location.replace("hdfs://hacluster", hiveSpecFsLocation);
            fs = HadoopUtil.getFileSystem(hiveSpecFsLocation);
        } else {
            fs = HadoopUtil.getFileSystem(location);
        }
        if (checkList) {
            fs.listStatus(new Path(location));
        }
        return fs.listStatusIterator(new Path(location));
    }

    @Override
    public Pair<TableDesc, TableExtDesc> loadTableMetadata(final String database, String tableName, String prj)
            throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager metaMgr = NTableMetadataManager.getInstance(config, prj);

        NSparkTableMeta tableMeta = getTableMetaExplorer().getSparkTableMeta(database, tableName);
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

        if (tableMeta.tableType != null) {
            tableDesc.setTableType(tableMeta.tableType);
        }
        //set table type = spark
        tableDesc.setSourceType(ISourceAware.ID_SPARK);
        tableDesc.setTransactional(tableMeta.isTransactional);
        tableDesc.setRangePartition(tableMeta.isRangePartition);
        tableDesc.setTableComment(tableMeta.tableComment);

        Set<String> partColumnSet = Optional.ofNullable(tableMeta.partitionColumns) //
                .orElseGet(Collections::emptyList).stream().map(field -> field.name) //
                .collect(Collectors.toSet());
        int columnNumber = tableMeta.allColumns.size();
        List<ColumnDesc> columns = new ArrayList<ColumnDesc>(columnNumber);
        for (int i = 0; i < columnNumber; i++) {
            NSparkTableMeta.SparkTableColumnMeta field = tableMeta.allColumns.get(i);
            ColumnDesc cdesc = new ColumnDesc();
            cdesc.setName(field.name.toUpperCase(Locale.ROOT));
            cdesc.setCaseSensitiveName(field.name);
            // use "double" in kylin for "float"
            if ("float".equalsIgnoreCase(field.dataType)) {
                cdesc.setDatatype("double");
            } else {
                cdesc.setDatatype(field.dataType);
            }
            cdesc.setId(String.valueOf(i + 1));
            cdesc.setComment(field.comment);
            cdesc.setPartitioned(partColumnSet.contains(field.name));
            columns.add(cdesc);
        }
        tableDesc.setColumns(columns.toArray(new ColumnDesc[columnNumber]));
        List<String> partCols = tableMeta.partitionColumns.stream().map(col -> col.name).collect(Collectors.toList());
        if (!partCols.isEmpty()) {
            tableDesc.setPartitionColumn(partCols.get(0).toUpperCase(Locale.ROOT));
        } else {
            tableDesc.setPartitionColumn(null);
        }
        StringBuilder partitionColumnBuilder = new StringBuilder();
        for (int i = 0, n = tableMeta.partitionColumns.size(); i < n; i++) {
            if (i > 0)
                partitionColumnBuilder.append(", ");
            partitionColumnBuilder.append(tableMeta.partitionColumns.get(i).name.toUpperCase(Locale.ROOT));
        }

        TableExtDesc tableExtDesc = new TableExtDesc();
        tableExtDesc.setIdentity(tableDesc.getIdentity());
        tableExtDesc.setUuid(RandomUtil.randomUUIDStr());
        tableExtDesc.setLastModified(0);
        tableExtDesc.init(prj);

        tableExtDesc.addDataSourceProp(TableExtDesc.LOCATION_PROPERTY_KEY, tableMeta.sdLocation);
        tableExtDesc.addDataSourceProp("owner", tableMeta.owner);
        tableExtDesc.addDataSourceProp("create_time", tableMeta.createTime);
        tableExtDesc.addDataSourceProp("last_access_time", tableMeta.lastAccessTime);
        tableExtDesc.addDataSourceProp("partition_column", partitionColumnBuilder.toString());
        tableExtDesc.addDataSourceProp("total_file_size", String.valueOf(tableMeta.fileSize));
        tableExtDesc.addDataSourceProp("total_file_number", String.valueOf(tableMeta.fileNum));
        tableExtDesc.addDataSourceProp("hive_inputFormat", tableMeta.sdInputFormat);
        tableExtDesc.addDataSourceProp("hive_outputFormat", tableMeta.sdOutputFormat);
        tableExtDesc.addDataSourceProp(TableExtDesc.S3_ROLE_PROPERTY_KEY, tableMeta.s3Role);
        tableExtDesc.addDataSourceProp(TableExtDesc.S3_ENDPOINT_KEY, tableMeta.s3Endpoint);
        return Pair.newPair(tableDesc, tableExtDesc);
    }

    @Override
    public List<String> getRelatedKylinResources(TableDesc table) {
        return Collections.emptyList();
    }

    @Override
    public boolean checkDatabaseAccess(String database) throws Exception {
        boolean hiveDBAccessFilterEnable = KapConfig.getInstanceFromEnv().getDBAccessFilterEnable();
        String viewDB = KylinConfig.getInstanceFromEnv().getDDLLogicalViewDB();
        if (viewDB.equalsIgnoreCase(database)) {
            return true;
        }
        if (hiveDBAccessFilterEnable) {
            logger.info("Check database {} access start.", database);
            try {
                Database db = SparderEnv.getSparkSession().catalog().getDatabase(database);
            } catch (AnalysisException e) {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                logger.error("The current user: {} does not have permission to access database {}", ugi.getUserName(),
                        database);
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean checkTablesAccess(Set<String> tables) {
        return tables.stream().allMatch(this::checkTableAccess);
    }

    @Override
    public Set<String> getTablePartitions(String database, String table, String prj, String partCol) {
        return getTableMetaExplorer().checkAndGetTablePartitions(database, table, partCol);
    }

    @Override
    public void createSampleDatabase(String database) throws Exception {
        SparderEnv.getSparkSession().sql(generateCreateSchemaSql(database));
    }

    @Override
    public void createSampleTable(TableDesc table) throws Exception {
        String[] createTableSqls = generateCreateTableSql(table);
        for (String sql : createTableSqls) {
            SparderEnv.getSparkSession().sql(sql);
        }
    }

    @Override
    public void loadSampleData(String tableName, String tableFileDir) throws Exception {
        Dataset<Row> dataset = SparderEnv.getSparkSession().read().csv(tableFileDir + "/" + tableName + ".csv").toDF();
        if (tableName.indexOf(".") > 0) {
            tableName = tableName.substring(tableName.indexOf(".") + 1);
        }
        dataset.createOrReplaceTempView(tableName);
    }

    @Override
    public void createWrapperView(String origTableName, String viewName) throws Exception {
        throw new UnsupportedOperationException("unsupport create wrapper view");
    }

    public String getLoc(SparkSession spark, String table, String hiveSpecFsLocation) {
        String loc = spark.sql("desc formatted " + table).where("col_name == 'Location'").head().getString(1);
        if (null == hiveSpecFsLocation || null == loc) {
            return loc;
        }
        return loc.replace("hdfs://hacluster", hiveSpecFsLocation);
    }
}

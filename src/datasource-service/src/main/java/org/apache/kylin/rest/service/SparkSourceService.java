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
package org.apache.kylin.rest.service;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.request.DDLRequest;
import org.apache.kylin.rest.response.DDLResponse;
import org.apache.kylin.rest.response.ExportTablesResponse;
import org.apache.kylin.rest.response.TableNameResponse;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DDLDesc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DdlOperation;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.Data;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import scala.Option;
import scala.collection.Iterator;

@Slf4j
@Service
public class SparkSourceService extends BasicService {

    //key in hive metadata map
    private static final String CHAR_VARCHAR_TYPE_STRING = "__CHAR_VARCHAR_TYPE_STRING";
    private static final String HIVE_COMMENT = "comment";

    // constants for samples
    private static final String TABLE_LINEORDER = "ssb.lineorder";
    private static final String VIEW_P_LINEORDER = "ssb.p_lineorder";
    private static final String CREATE_VIEW_P_LINEORDER = "create view if not exists SSB.P_LINEORDER as\n"
            + "        select LO_ORDERKEY,\n" + "        LO_LINENUMBER,\n" + "        LO_CUSTKEY,\n"
            + "        LO_PARTKEY,\n" + "        LO_SUPPKEY,\n" + "        LO_ORDERDATE,\n"
            + "        LO_ORDERPRIOTITY,\n" + "        LO_SHIPPRIOTITY,\n" + "        LO_QUANTITY,\n"
            + "        LO_EXTENDEDPRICE,\n" + "        LO_ORDTOTALPRICE,\n" + "        LO_DISCOUNT,\n"
            + "        LO_REVENUE,\n" + "        LO_SUPPLYCOST,\n" + "        LO_TAX,\n" + "        LO_COMMITDATE,\n"
            + "        LO_SHIPMODE,\n" + "        LO_EXTENDEDPRICE*LO_DISCOUNT as V_REVENUE\n"
            + "        from SSB.LINEORDER";

    public DDLResponse executeSQL(DDLRequest request) {
        List<String> sqlList = Arrays.asList(request.getSql().split(";"));
        if (!Strings.isNullOrEmpty(request.getDatabase())) {
            executeSQL("use " + request.getDatabase());
        }
        DDLResponse ddlResponse = new DDLResponse();
        Map<String, DDLDesc> succeed = Maps.newHashMap();
        Map<String, String> failed = Maps.newHashMap();
        sqlList.forEach(s -> {
            if (!Strings.isNullOrEmpty(s)) {
                try {
                    DDLDesc ddlDesc = executeSQL(s);
                    succeed.put(s, ddlDesc);
                } catch (Exception e) {
                    log.error("Failed to execute sql[{}]", s, e);
                    failed.put(s, e.getMessage());
                }
            }
        });
        ddlResponse.setSucceed(succeed);
        ddlResponse.setFailed(failed);
        return ddlResponse;
    }

    public DDLDesc executeSQL(String sql) {
        return DdlOperation.executeSQL(sql);
    }

    public void dropTable(String database, String table) throws AnalysisException {
        SparkSession ss = SparderEnv.getSparkSession();
        if (ss.catalog().tableExists(database, table)) {
            val t = ss.catalog().getTable(database, table);
            if ("view".equalsIgnoreCase(t.tableType())) {
                ss.sql(String.format(Locale.ROOT, "drop view %s.%s", database, table));
            } else {
                ss.sql(String.format(Locale.ROOT, "drop table %s.%s", database, table));
            }
        }
    }

    public List<String> listDatabase() {
        SparkSession sparkSession = SparderEnv.getSparkSession();
        Dataset<Database> databaseSet = sparkSession.catalog().listDatabases();
        List<Database> databases = databaseSet.collectAsList();
        return databases.stream().map(database -> database.name().toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
    }

    public List<TableNameResponse> listTables(String db, String project) throws Exception {
        if (Strings.isNullOrEmpty(project)) {
            SparkSession sparkSession = SparderEnv.getSparkSession();
            Dataset<Table> tableDataset = sparkSession.catalog().listTables(db);
            List<Table> sparkTables = tableDataset.collectAsList();
            return sparkTables.stream()
                    .map(table -> new TableNameResponse(table.name().toUpperCase(Locale.ROOT), false))
                    .collect(Collectors.toList());
        }
        ISourceMetadataExplorer explr = SourceFactory.getSource(getManager(NProjectManager.class).getProject(project))
                .getSourceMetadataExplorer();
        List<String> tables = explr.listTables(db).stream().map(str -> str.toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
        List<TableNameResponse> tableNameResponses = Lists.newArrayList();
        tables.forEach(table -> {
            TableNameResponse response = new TableNameResponse();
            response.setLoaded(getManager(NTableMetadataManager.class, project).getTableDesc(db + "." + table) != null);
            response.setTableName(table);
            tableNameResponses.add(response);
        });
        return tableNameResponses;
    }

    public List<ColumnModel> listColumns(String db, String table) {
        SparkSession sparkSession = SparderEnv.getSparkSession();
        CatalogTable catalogTable = sparkSession.sessionState().catalog()
                .getTempViewOrPermanentTableMetadata(new TableIdentifier(table, Option.apply(db)));
        scala.collection.immutable.List<StructField> structFieldList = catalogTable.schema().toList();
        Iterator<StructField> structFieldIterator = structFieldList.iterator();

        List<ColumnModel> columnModels = Lists.newArrayList();
        while (structFieldIterator.hasNext()) {
            StructField structField = structFieldIterator.next();
            String name = structField.name();
            String datatype = structField.dataType().simpleString();

            Metadata metadata = structField.metadata();

            ColumnModel columnModel = new ColumnModel();
            if (catalogTable.partitionColumnNames().contains(name)) {
                columnModel.setPartition(true);
            }
            columnModel.setName(name);
            columnModel.setDescription(metadata.contains(HIVE_COMMENT) ? metadata.getString(HIVE_COMMENT) : "");
            //use hive datatype if it exists , otherwise use spark datatype
            columnModel.setDataType(
                    metadata.contains(CHAR_VARCHAR_TYPE_STRING) ? metadata.getString(CHAR_VARCHAR_TYPE_STRING)
                            : datatype);
            columnModels.add(columnModel);
        }

        return columnModels;
    }

    public String getTableDesc(String database, String table) {
        return DdlOperation.getTableDesc(database, table);
    }

    public ExportTablesResponse exportTables(String database, String[] tables) {
        if (database == null || database.equals("")) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, MsgPicker.getMsg().getEmptyDatabase());
        }
        if (tables.length == 0) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, MsgPicker.getMsg().getEmptyTableList());
        }
        if (!databaseExists(database)) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getDatabaseNotExist(), database));
        }
        val tableResponse = new ExportTablesResponse();
        Map<String, String> tableDesc = Maps.newHashMap();
        for (String table : tables) {
            if (table.equals("")) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER, MsgPicker.getMsg().getEmptyTableList());
            }
            if (!tableExists(database, table)) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                        String.format(Locale.ROOT, MsgPicker.getMsg().getTableNotFound(), table));
            }
            tableDesc.put(table, DdlOperation.getTableDesc(database, table).replaceAll("\t|\r|\n", " "));
        }
        tableResponse.setDatabase(database);
        tableResponse.setTables(tableDesc);
        return tableResponse;
    }

    public boolean databaseExists(String database) {
        SparkSession sparkSession = SparderEnv.getSparkSession();
        return sparkSession.catalog().databaseExists(database);
    }

    public boolean tableExists(String database, String table) {
        SparkSession sparkSession = SparderEnv.getSparkSession();
        return sparkSession.catalog().tableExists(database, table);
    }

    public boolean hasPartition(String database, String table) {
        return DdlOperation.hasPartition(database, table);
    }

    public List<String> msck(String database, String table) {
        return DdlOperation.msck(database, table);
    }

    public List<String> loadSamples(SparkSession ss, SaveMode mode) throws IOException, InterruptedException {
        Lock lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().getLockForCurrentThread("samples");
        //list samples and use file-name as table name
        List<File> fileList = listSampleFiles();
        List<String> createdTables = Lists.newArrayList();
        lock.tryLock(60, TimeUnit.SECONDS);
        try {
            for (File file : fileList) {
                if (!file.isDirectory()) {
                    continue;
                }
                String fileName = file.getName(), table = fileName, tableName = table, db = "DEFAULT";
                if (fileName.contains(".")) {
                    String[] splits = fileName.split("\\.");
                    db = splits[0];
                    tableName = splits[1];
                    if (!ss.catalog().databaseExists(db)) {
                        ss.sql("create database if not exists " + db);
                    }
                } else {
                    table = String.format(Locale.ROOT, "%s.%s", db, table);
                }
                if (!ss.catalog().tableExists(table)) {
                    loadSamples(ss, mode, table, tableName, db, file, fileName);
                }
                createdTables.add(table);
            }
            if (ss.catalog().tableExists(TABLE_LINEORDER)) {
                ss.sql(CREATE_VIEW_P_LINEORDER);
                createdTables.add(VIEW_P_LINEORDER);
            }
            log.info("Load samples {} successfully", StringHelper.join(createdTables, ","));
        } finally {
            lock.unlock();
        }
        return createdTables;
    }

    private void loadSamples(SparkSession ss, SaveMode mode, String table, String tableName, String db, File file,
            String fileName) throws IOException {
        String filePath = file.getAbsolutePath();
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        String hdfsPath = String.format(Locale.ROOT, "/tmp/%s", fileName);
        try {
            log.debug("Copy from {} to {}", filePath, hdfsPath);
            File[] parquetFiles = file.listFiles();
            if (parquetFiles != null) {
                for (File parquetFile : parquetFiles) {
                    fileSystem.copyFromLocalFile(new Path(parquetFile.getAbsolutePath()), new Path(hdfsPath));
                }
            }
            // KC-6666, check and delete location
            String tbLocation = String.format(Locale.ROOT, "%s/%s", ss.catalog().getDatabase(db).locationUri(),
                    tableName);
            FileSystem fs = FileSystem.get(ss.sparkContext().hadoopConfiguration());
            Path path = new Path(tbLocation);
            if (fs.exists(path)) {
                log.debug("Delete existed table location {}", path.toString());
                fs.delete(path, true);
            }
            ss.read().parquet(hdfsPath).write().mode(mode).saveAsTable(table);
        } catch (Exception e) {
            log.error("Load sample {} failed.", fileName, e);
            throw new IllegalStateException(String.format(Locale.ROOT, "Load sample %s failed", fileName), e);
        } finally {
            fileSystem.delete(new Path(hdfsPath), false);
        }
    }

    public List<String> loadSamples() throws IOException, InterruptedException {
        log.info("Start to load samples");
        SparkSession ss = SparderEnv.getSparkSession();
        return loadSamples(ss, SaveMode.Overwrite);
    }

    private List<File> listSampleFiles() {
        //class_path samples
        String sampleDir = "../samples";
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            sampleDir = "../../build/samples";
        }
        File file = new File(sampleDir);
        log.debug("Samples file path is {}", file.getAbsolutePath());
        File[] listFiles = file.listFiles();
        if (!file.exists() || null == listFiles) {
            throw new RuntimeException("No sample data found.");
        }
        return Arrays.asList(listFiles);
    }

    @Data
    public static class ColumnModel {

        @JsonProperty("name")
        private String name;
        @JsonProperty("description")
        private String description;
        @JsonProperty("dataType")
        private String dataType;
        @JsonProperty("partition")
        private boolean partition;
    }
}

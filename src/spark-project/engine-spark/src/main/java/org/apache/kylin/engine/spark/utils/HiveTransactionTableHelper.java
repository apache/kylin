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

package org.apache.kylin.engine.spark.utils;

import static org.apache.kylin.common.exception.ServerErrorCode.READ_TRANSACTIONAL_TBALE_FAILED;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.engine.spark.job.KylinBuildEnv;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.hive.HiveCmdBuilder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveTransactionTableHelper {
    private HiveTransactionTableHelper() {
    }

    public static String doGetQueryHiveTemporaryTableSql(TableDesc table, Map<String, String> params, String colString,
            KylinBuildEnv kylinBuildEnv) {
        String sql = "";
        String jobId = kylinBuildEnv.buildJobInfos().getJobId();
        String project = kylinBuildEnv.buildJobInfos().getProject();
        KylinConfig kylinConfig = kylinBuildEnv.kylinConfig();

        String dir = kylinConfig.getJobTmpTransactionalTableDir(project, jobId);
        // jobId: 80c95c04-4291-9f95-3c0f-0b014f7f14af-ad53bba1-e9f2-cee2-5b21-21f2e3a73312
        // tableSuffix: 80c95c04-4291 => 80c95c044291
        String tableSuffix = StringUtils.replace(StringUtils.substring(jobId, 0, 13), "-", "");
        String tempWritableDB = kylinBuildEnv.kylinConfig().getBuildResourceTemporaryWritableDB();
        String tempTableName = table.getTransactionalTableIdentity(tempWritableDB).concat(tableSuffix);
        String tempBackTickTableName = table.getBackTickTransactionalTableIdentity(tempWritableDB, tableSuffix);
        String tableDir = getTableDir(tempTableName, dir);
        checkInterTableExistFirst(table, params, kylinBuildEnv, jobId, dir, tableSuffix, tempTableName, tableDir);
        sql = checkInterTableExistSecondAndGetSql(table, params, colString, jobId, tempBackTickTableName, tableDir);
        return sql;
    }

    public static String checkInterTableExistSecondAndGetSql(TableDesc table, Map<String, String> params,
            String colString, String jobId, String tempTableName, String tableDir) {
        boolean secondCheck = checkInterTableExist(tableDir);
        log.info("second check is table ready : {} ", secondCheck);
        if (secondCheck) {
            log.info("table ready,start build sql");
            String queryCondition = generateTxTableQueryCondition(table, params);
            return String.format(Locale.ROOT, "select %s from %s %s", colString, tempTableName, queryCondition);
        } else {
            throw new KylinException(READ_TRANSACTIONAL_TBALE_FAILED,
                    String.format(Locale.ROOT, "Can't read transactional table, jobId %s.", jobId));
        }
    }

    public static void checkInterTableExistFirst(TableDesc table, Map<String, String> params,
            KylinBuildEnv kylinBuildEnv, String jobId, String dir, String tableSuffix, String tempTableName,
            String tableDir) {
        boolean firstCheck = checkInterTableExist(tableDir);
        log.info("first check is table ready : {} ", firstCheck);
        if (!firstCheck) {
            try {
                createHiveTableDirIfNeeded(dir, tempTableName);
            } catch (IOException ioException) {
                log.error(READ_TRANSACTIONAL_TBALE_FAILED.name(), ioException);
                throw new KylinException(READ_TRANSACTIONAL_TBALE_FAILED,
                        String.format(Locale.ROOT, "Can't create hive table dir, jobId %s.", jobId));
            }
            generateTxTable(kylinBuildEnv, table, tableSuffix, params, tableDir);
        }
    }

    private static final String QUOTE = "`";

    private static String doQuote(String identifier) {
        return QUOTE + identifier + QUOTE;
    }

    public static String generateHiveInitStatements(String flatTableDatabase) {
        if (StringUtils.isEmpty(flatTableDatabase)) {
            log.info("database name is empty.");
            return "";
        }
        return "USE " + doQuote(flatTableDatabase) + ";\n";
    }

    public static String generateInsertDataStatement(ColumnDesc[] columnDescs, String originDB, String originTable,
            String interTable, String queryCondition) {
        final String sep = "\n";
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + sep);
        for (int i = 0; i < columnDescs.length; i++) {
            ColumnDesc col = columnDescs[i];
            if (i > 0) {
                sql.append(",");
            }
            sql.append(doQuote(col.getName())).append(sep);
        }
        sql.append("FROM ").append(doQuote(originDB)).append(".").append(doQuote(originTable)).append(" ")
                .append(queryCondition).append(sep);
        return "INSERT OVERWRITE TABLE " + doQuote(interTable) + " " + sql.toString() + ";\n";
    }

    public static String getCreateTableStatement(TableDesc tableDesc, String interTable, ColumnDesc[] columnDescs,
            String tableDir, String storageFormat, String fieldDelimiter, String queryCondition) {
        String originDB = tableDesc.getDatabase();
        String originTable = tableDesc.getName();
        return generateDropTableStatement(interTable)
                + generateCreateTableStatement(interTable, tableDir, columnDescs, storageFormat, fieldDelimiter)
                + generateInsertDataStatement(columnDescs, originDB, originTable, interTable, queryCondition);
    }

    public static String generateDropTableStatement(String interTable) {
        return "DROP TABLE IF EXISTS " + doQuote(interTable) + ";" + "\n";
    }

    public static String generateCreateTableStatement(String interTable, String tableDir, ColumnDesc[] columnDescs,
            String storageFormat, String fieldDelimiter) {
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE EXTERNAL TABLE IF NOT EXISTS ").append(doQuote(interTable)).append("\n");
        ddl.append("(" + "\n");
        for (int i = 0; i < columnDescs.length; i++) {
            ColumnDesc col = columnDescs[i];
            if (i > 0) {
                ddl.append(",");
            }
            ddl.append(doQuote(col.getName())).append(" ").append(getHiveDataType(col.getDatatype())).append("\n");
        }
        ddl.append(")" + "\n");
        if ("TEXTFILE".equalsIgnoreCase(storageFormat)) {
            ddl.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '")
                    .append(StringEscapeUtils.escapeJava(fieldDelimiter)).append("'\n");
        }
        ddl.append("STORED AS ").append(storageFormat).append("\n");
        ddl.append("LOCATION '").append(tableDir).append("';").append("\n");
        ddl.append("ALTER TABLE ").append(doQuote(interTable)).append(" SET TBLPROPERTIES('auto.purge'='true');\n");
        return ddl.toString();
    }

    public static String getTableDir(String tableName, String storageDfsDir) {
        if (storageDfsDir.endsWith("/")) {
            return storageDfsDir.concat(tableName);
        }
        return storageDfsDir.concat("/").concat(tableName);
    }

    public static String getHiveDataType(String javaDataType) {
        String originDataType = javaDataType.toLowerCase(Locale.ROOT);
        String hiveDataType;
        if (originDataType.startsWith("varchar")) {
            hiveDataType = "string";
        } else if (originDataType.startsWith("integer")) {
            hiveDataType = "int";
        } else if (originDataType.startsWith("bigint")) {
            hiveDataType = "bigint";
        } else {
            hiveDataType = originDataType;
        }

        return hiveDataType;
    }

    static boolean checkInterTableExist(String tableDir) {
        try {
            log.info("check intermediate table dir : {}", tableDir);
            Path path = new Path(tableDir);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(path)) {
                return true;
            }
        } catch (IOException e) {
            Throwables.propagate(e);
        }
        return false;
    }

    public static void generateTxTable(KylinBuildEnv kylinBuildEnv, TableDesc table, String tableSuffix,
            Map<String, String> params, String tableDir) {
        String jobId = kylinBuildEnv.buildJobInfos().getJobId();
        log.info("job wait for generate intermediate table, job id : {}", jobId);
        KylinConfig config = kylinBuildEnv.kylinConfig();
        String database = determineDBUsed(kylinBuildEnv, table);

        ColumnDesc[] filtered = Arrays.stream(table.getColumns()).filter(t -> !t.isComputedColumn())
                .toArray(ColumnDesc[]::new);

        String queryCondition = generateTxTableQueryCondition(table, params);

        final HiveCmdBuilder hiveCmdBuilder = new HiveCmdBuilder(config);
        hiveCmdBuilder.addStatement(generateHiveInitStatements(database));
        String createTableStatement = getCreateTableStatement(table,
                table.getTransactionalTableName().concat(tableSuffix), filtered, tableDir,
                config.getFlatTableStorageFormat(), config.getFlatTableFieldDelimiter(), queryCondition);
        hiveCmdBuilder.addStatement(createTableStatement);
        final String cmd = hiveCmdBuilder.toString();
        CliCommandExecutor cliCommandExecutor = new CliCommandExecutor();
        try {
            CliCommandExecutor.CliCmdExecResult result = cliCommandExecutor.execute(cmd, null);
            if (result.getCode() != 0) {
                log.error("execute create intermediate table return fail, jobId : {}", jobId);
            } else {
                log.info("execute create intermediate table succeeded, jobId : {}", jobId);
            }
        } catch (ShellException e) {
            log.error("failed to execute create intermediate table, jobId : {}, result : {}", jobId, e);
        }
    }

    static String determineDBUsed(KylinBuildEnv kylinBuildEnv, TableDesc table) {
        String database = table.getCaseSensitiveDatabase().endsWith("null") ? "default"
                : table.getCaseSensitiveDatabase();
        String temporaryWritableDB = kylinBuildEnv.kylinConfig().getBuildResourceTemporaryWritableDB();
        if (StringUtils.isNotBlank(temporaryWritableDB)) {
            database = temporaryWritableDB;
        }
        return database.toUpperCase(Locale.ROOT);
    }

    /**
     * generate transaction table query condition
     *
     * @param table  the table description
     * @param params the param map
     * @return table query condition string
     */
    private static String generateTxTableQueryCondition(TableDesc table, Map<String, String> params) {
        log.info("table ready,start build sql");
        PartitionDesc partitionDesc = table.getPartitionDesc();
        log.info("table partition desc is :{}", partitionDesc);
        log.info("whether partition query is required ? :{}", Objects.nonNull(partitionDesc));
        log.info("table segment range start: {}; end: {}", params.get("segmentStart"), params.get("segmentEnd"));

        String sql = "";
        Boolean hasParam = Objects.nonNull(partitionDesc) && params.containsKey("segmentStart")
                && params.containsKey("segmentEnd") && Objects.nonNull(partitionDesc.getPartitionDateColumnRef())
                && Objects.nonNull(partitionDesc.getPartitionDateColumnRef().getTable())
                && partitionDesc.getPartitionDateColumnRef().getTable().equalsIgnoreCase(table.getIdentity());
        if (hasParam) {
            ColumnDesc partitionColumnDesc = partitionDesc.getPartitionDateColumnRef().getColumnDesc();
            String partitionDataColumn = partitionColumnDesc.getName();
            log.info("table partition column name is :{}", partitionDataColumn);
            String columnDataTypeName = partitionColumnDesc.getType().getName();
            log.info("table partition column data type is :{}", columnDataTypeName);

            boolean intDataTypeFlag = columnDataTypeName.equalsIgnoreCase(DataType.INT)
                    || columnDataTypeName.equalsIgnoreCase(DataType.INTEGER);

            boolean dateDataTypeFlag = columnDataTypeName.equalsIgnoreCase(DataType.TIME)
                    || columnDataTypeName.equalsIgnoreCase(DataType.TIMESTAMP)
                    || columnDataTypeName.equalsIgnoreCase(DataType.DATE)
                    || columnDataTypeName.equalsIgnoreCase(DataType.DATETIME);

            boolean strDataTypeFlag = columnDataTypeName.equalsIgnoreCase(DataType.STRING)
                    || columnDataTypeName.equalsIgnoreCase(DataType.VARCHAR);

            if (intDataTypeFlag || dateDataTypeFlag || strDataTypeFlag) {
                String partitionDateFormat = partitionDesc.getPartitionDateFormat();
                log.info("table partition data format is :{}", partitionDateFormat);
                String beginDate = DateFormat.formatToDateStr(Long.parseLong(params.getOrDefault("segmentStart", "0")),
                        partitionDateFormat);
                String endDate = DateFormat.formatToDateStr(Long.parseLong(params.getOrDefault("segmentEnd", "0")),
                        partitionDateFormat);
                log.info("segment range is :[{},{}]", beginDate, endDate);
                if (intDataTypeFlag) {
                    sql = String.format(Locale.ROOT, "WHERE %s BETWEEN %d AND %d", doQuote(partitionDataColumn),
                            Integer.parseInt(beginDate), Integer.parseInt(endDate));
                } else {
                    sql = String.format(Locale.ROOT, "WHERE %s BETWEEN '%s' AND '%s'", doQuote(partitionDataColumn),
                            beginDate, endDate);
                }
            }
        }

        String sampleRowCount = params.getOrDefault("sampleRowCount", "");
        if (!sampleRowCount.isEmpty()) {
            sql = String.format(Locale.ROOT, " limit %d", Integer.parseInt(sampleRowCount));
        }

        return sql;
    }

    public static void createHiveTableDirIfNeeded(String jobWorkingDir, String tableName) throws IOException {
        // Create work dir to avoid hive create it, the difference is that the owners are different.
        // if create first, the privilege of dir is kylin user, otherwise is hive user
        Path path = new Path(jobWorkingDir, tableName);
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();

        if (!fileSystem.exists(path)) {
            log.info("Create hive table dir in hdfs: {}: ", path);
        } else {
            log.info("Hive table dir already exists in hdfs: {}, delete old dir and recreate it", path);
            fileSystem.delete(path, true);
        }
        fileSystem.mkdirs(path);
        fileSystem.setPermission(path, new FsPermission((short) 00777));
    }
}

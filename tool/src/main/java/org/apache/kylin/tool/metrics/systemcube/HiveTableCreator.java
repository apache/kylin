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

package org.apache.kylin.tool.metrics.systemcube;

import java.util.List;

import java.util.Locale;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.apache.kylin.metrics.lib.impl.hive.HiveProducerRecord;
import org.apache.kylin.metrics.lib.impl.hive.HiveReservoirReporter;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.apache.kylin.metrics.property.QuerySparkExecutionEnum;
import org.apache.kylin.metrics.property.QuerySparkJobEnum;
import org.apache.kylin.metrics.property.QuerySparkStageEnum;
import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class HiveTableCreator {

    public static void main(String[] args) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        System.out.println(generateAllSQL(config));
    }

    public static String generateAllSQL(KylinConfig config) {
        StringBuilder sb = new StringBuilder();
        sb.append(generateDatabaseSQL());
        sb.append("\n");
        sb.append(generateHiveTableSQLForMetricsQuery(config));
        sb.append("\n");
        sb.append(generateHiveTableSQLForMetricsQueryCUBE(config));
        sb.append("\n");
        sb.append(generateHiveTableSQLForMetricsQueryRPC(config));
        sb.append("\n");
        sb.append(generateHiveTableSQLForMetricsJob(config));
        sb.append("\n");
        sb.append(generateHiveTableSQLForMetricsJobException(config));

        return sb.toString();
    }

    public static String generateDatabaseSQL() {
        return "CREATE DATABASE IF NOT EXISTS " + ActiveReservoirReporter.KYLIN_PREFIX + ";\n";
    }

    public static String generateHiveTableSQL(String tableName, List<Pair<String, String>> columns,
            List<Pair<String, String>> partitionKVs) {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP TABLE IF EXISTS " + tableName + ";\n");
        sb.append("\n");
        sb.append("CREATE TABLE " + tableName + "\n");
        sb.append("(\n");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            Pair<String, String> column = columns.get(i);
            sb.append(column.getFirst() + " " + column.getSecond() + "\n");
        }
        sb.append(")\n");
        if (partitionKVs != null && partitionKVs.size() > 0) {
            sb.append("PARTITIONED BY(");
            for (int i = 0; i < partitionKVs.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
                Pair<String, String> partitionKV = partitionKVs.get(i);
                sb.append(partitionKV.getFirst() + " " + partitionKV.getSecond());
            }
            sb.append(")\n");
        }
        sb.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY '" + HiveProducerRecord.DELIMITER + "'\n");
        sb.append("STORED AS TEXTFILE;\n");
        return sb.toString();
    }

    public static String generateHiveTableSQLForMetricsQuery(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectQueryExecution());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsQueryExecution(), getPartitionKVsForHiveTable());
    }

    public static String generateHiveTableSQLForMetricsQueryCUBE(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectQuerySparkJob());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsQuerySparkJob(), getPartitionKVsForHiveTable());
    }

    public static String generateHiveTableSQLForMetricsQueryRPC(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectQuerySparkStage());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsQuerySparkStage(), getPartitionKVsForHiveTable());
    }

    public static String generateHiveTableSQLForMetricsJob(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectJob());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsJob(), getPartitionKVsForHiveTable());
    }

    public static String generateHiveTableSQLForMetricsJobException(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectJobException());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsJobException(), getPartitionKVsForHiveTable());
    }

    public static List<Pair<String, String>> getHiveColumnsForMetricsQueryExecution() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(QuerySparkExecutionEnum.ID_CODE.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.QUERY_ID.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.EXECUTION_ID.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.HOST.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.USER.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.SPARDER_NAME.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.PROJECT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.REALIZATION.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.REALIZATION_TYPE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.CUBOID_IDS.toString(), HiveTypeEnum.HSTRING.toString()));

        columns.add(new Pair<>(QuerySparkExecutionEnum.TYPE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.START_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.END_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.add(new Pair<>(QuerySparkExecutionEnum.EXCEPTION.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.TIME_COST.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.TOTAL_SCAN_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.TOTAL_SCAN_BYTES.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.RESULT_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.add(new Pair<>(QuerySparkExecutionEnum.EXECUTION_DURATION.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.RESULT_SIZE.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.EXECUTOR_DESERIALIZE_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.EXECUTOR_RUN_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.EXECUTOR_CPU_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.JVM_GC_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.RESULT_SERIALIZATION_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.MEMORY_BYTE_SPILLED.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.DISK_BYTES_SPILLED.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkExecutionEnum.PEAK_EXECUTION_MEMORY.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.addAll(getTimeColumnsForMetrics());
        return columns;
    }

    public static List<Pair<String, String>> getHiveColumnsForMetricsQuerySparkJob() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(QuerySparkJobEnum.QUERY_ID.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.PROJECT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.EXECUTION_ID.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.JOB_ID.toString(), HiveTypeEnum.HINT.toString()));
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.HOST.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.START_TIME.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.END_TIME.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.IF_SUCCESS.toString(), HiveTypeEnum.HBOOLEAN.toString()));

        columns.add(new Pair<>(QuerySparkJobEnum.RESULT_SIZE.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.EXECUTOR_DESERIALIZE_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.EXECUTOR_RUN_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.EXECUTOR_CPU_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.JVM_GC_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.RESULT_SERIALIZATION_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.MEMORY_BYTE_SPILLED.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.DISK_BYTES_SPILLED.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkJobEnum.PEAK_EXECUTION_MEMORY.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.addAll(getTimeColumnsForMetrics());
        return columns;
    }

    public static List<Pair<String, String>> getHiveColumnsForMetricsQuerySparkStage() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.HOST.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.QUERY_ID.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.EXECUTION_ID.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.JOB_ID.toString(), HiveTypeEnum.HINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.STAGE_ID.toString(), HiveTypeEnum.HINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.SUBMIT_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.PROJECT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.REALIZATION.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.CUBOID_ID.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.IF_SUCCESS.toString(), HiveTypeEnum.HBOOLEAN.toString()));

        columns.add(new Pair<>(QuerySparkStageEnum.RESULT_SIZE.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.EXECUTOR_DESERIALIZE_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.EXECUTOR_DESERIALIZE_CPU_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.EXECUTOR_RUN_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.EXECUTOR_CPU_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.JVM_GC_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.RESULT_SERIALIZATION_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.MEMORY_BYTE_SPILLED.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.DISK_BYTES_SPILLED.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QuerySparkStageEnum.PEAK_EXECUTION_MEMORY.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.addAll(getTimeColumnsForMetrics());
        return columns;
    }

    public static List<Pair<String, String>> getHiveColumnsForMetricsJob() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(JobPropertyEnum.ID_CODE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.HOST.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.USER.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.PROJECT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.CUBE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.TYPE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.ALGORITHM.toString(), HiveTypeEnum.HSTRING.toString()));

        columns.add(new Pair<>(JobPropertyEnum.BUILD_DURATION.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(JobPropertyEnum.SOURCE_SIZE.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(JobPropertyEnum.CUBE_SIZE.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(JobPropertyEnum.PER_BYTES_TIME_COST.toString(), HiveTypeEnum.HDOUBLE.toString()));
        columns.add(new Pair<>(JobPropertyEnum.WAIT_RESOURCE_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.add(
                new Pair<>(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(JobPropertyEnum.STEP_DURATION_DICTIONARY.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(JobPropertyEnum.STEP_DURATION_INMEM_CUBING.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(
                new Pair<>(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.addAll(getTimeColumnsForMetrics());
        return columns;
    }

    public static List<Pair<String, String>> getHiveColumnsForMetricsJobException() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(JobPropertyEnum.ID_CODE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.HOST.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.USER.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.PROJECT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.CUBE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.TYPE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(JobPropertyEnum.ALGORITHM.toString(), HiveTypeEnum.HSTRING.toString()));

        columns.add(new Pair<>(JobPropertyEnum.EXCEPTION.toString(), HiveTypeEnum.HSTRING.toString()));

        columns.addAll(getTimeColumnsForMetrics());
        return columns;
    }

    public static List<Pair<String, String>> getPartitionKVsForHiveTable() {
        List<Pair<String, String>> partitionKVs = Lists.newLinkedList();
        partitionKVs.add(new Pair<>(TimePropertyEnum.DAY_DATE.toString(), HiveTypeEnum.HSTRING.toString()));
        return partitionKVs;
    }

    public static List<Pair<String, String>> getTimeColumnsForMetrics() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(TimePropertyEnum.YEAR.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(TimePropertyEnum.MONTH.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(TimePropertyEnum.WEEK_BEGIN_DATE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(TimePropertyEnum.DAY_TIME.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(TimePropertyEnum.TIME_HOUR.toString(), HiveTypeEnum.HINT.toString()));
        columns.add(new Pair<>(TimePropertyEnum.TIME_MINUTE.toString(), HiveTypeEnum.HINT.toString()));
        columns.add(new Pair<>(TimePropertyEnum.TIME_SECOND.toString(), HiveTypeEnum.HINT.toString()));

        return columns;
    }

    enum HiveTypeEnum {
        HBOOLEAN("boolean"), HINT("int"), HBIGINT("bigint"), HDOUBLE("double"), HSTRING("string"), HDECIMAL("decimal");

        private final String typeName;

        HiveTypeEnum(String typeName) {
            this.typeName = typeName;
        }

        public static HiveTypeEnum getByTypeName(String typeName) {
            if (Strings.isNullOrEmpty(typeName)) {
                return null;
            }
            for (HiveTypeEnum hiveType : HiveTypeEnum.values()) {
                if (hiveType.typeName.equals(typeName.toLowerCase(Locale.ROOT))) {
                    return hiveType;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return typeName;
        }
    }
}

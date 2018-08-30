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
import org.apache.kylin.metrics.property.QueryCubePropertyEnum;
import org.apache.kylin.metrics.property.QueryPropertyEnum;
import org.apache.kylin.metrics.property.QueryRPCPropertyEnum;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class HiveTableCreator {

    public static void main(String[] args) {
        //        KylinConfig.setSandboxEnvIfPossible();
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
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectQuery());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsQuery(), getPartitionKVsForHiveTable());
    }

    public static String generateHiveTableSQLForMetricsQueryCUBE(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectQueryCube());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsQueryCube(), getPartitionKVsForHiveTable());
    }

    public static String generateHiveTableSQLForMetricsQueryRPC(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectQueryRpcCall());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsQueryRPC(), getPartitionKVsForHiveTable());
    }

    public static String generateHiveTableSQLForMetricsJob(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectJob());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsJob(), getPartitionKVsForHiveTable());
    }

    public static String generateHiveTableSQLForMetricsJobException(KylinConfig config) {
        String tableName = HiveReservoirReporter.getTableFromSubject(config.getKylinMetricsSubjectJobException());
        return generateHiveTableSQL(tableName, getHiveColumnsForMetricsJobException(), getPartitionKVsForHiveTable());
    }

    public static List<Pair<String, String>> getHiveColumnsForMetricsQuery() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(QueryPropertyEnum.ID_CODE.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.HOST.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.USER.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.PROJECT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.REALIZATION.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.REALIZATION_TYPE.toString(), HiveTypeEnum.HINT.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.TYPE.toString(), HiveTypeEnum.HSTRING.toString()));

        columns.add(new Pair<>(QueryPropertyEnum.EXCEPTION.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.TIME_COST.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.CALCITE_RETURN_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.STORAGE_RETURN_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryPropertyEnum.AGGR_FILTER_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.addAll(getTimeColumnsForMetrics());
        return columns;
    }

    public static List<Pair<String, String>> getHiveColumnsForMetricsQueryCube() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.HOST.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.PROJECT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.CUBE.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.SEGMENT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.CUBOID_SOURCE.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.CUBOID_TARGET.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.IF_MATCH.toString(), HiveTypeEnum.HBOOLEAN.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.FILTER_MASK.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.IF_SUCCESS.toString(), HiveTypeEnum.HBOOLEAN.toString()));

        columns.add(new Pair<>(QueryCubePropertyEnum.WEIGHT_PER_HIT.toString(), HiveTypeEnum.HDOUBLE.toString()));

        columns.add(new Pair<>(QueryCubePropertyEnum.CALL_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.TIME_SUM.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.TIME_MAX.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.SKIP_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.SCAN_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.RETURN_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.AGGR_FILTER_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryCubePropertyEnum.AGGR_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));

        columns.addAll(getTimeColumnsForMetrics());
        return columns;
    }

    public static List<Pair<String, String>> getHiveColumnsForMetricsQueryRPC() {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.add(new Pair<>(RecordEvent.RecordReserveKeyEnum.HOST.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.PROJECT.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.REALIZATION.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.RPC_SERVER.toString(), HiveTypeEnum.HSTRING.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.EXCEPTION.toString(), HiveTypeEnum.HSTRING.toString()));

        columns.add(new Pair<>(QueryRPCPropertyEnum.CALL_TIME.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.RETURN_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.SCAN_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.SKIP_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.AGGR_FILTER_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));
        columns.add(new Pair<>(QueryRPCPropertyEnum.AGGR_COUNT.toString(), HiveTypeEnum.HBIGINT.toString()));

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

        public String toString() {
            return typeName;
        }
    }
}

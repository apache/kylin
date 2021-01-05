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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.tool.metrics.systemcube.def.MetricsSinkDesc;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class KylinTableCreator {

    public static void main(String[] args) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        TableDesc kylinTable = generateKylinTableForMetricsQueryExecution(config, new MetricsSinkDesc());
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        TableMetadataManager.TABLE_SERIALIZER.serialize(kylinTable, dout);
        dout.close();
        buf.close();
        System.out.println(buf.toString("UTF-8"));
    }

    public static TableDesc generateKylinTableForMetricsQueryExecution(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQueryExecution());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectQueryExecution(), columns);
    }

    public static TableDesc generateKylinTableForMetricsQuerySparkJob(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQuerySparkJob());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectQuerySparkJob(), columns);
    }

    public static TableDesc generateKylinTableForMetricsQuerySparkStage(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQuerySparkStage());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectQuerySparkStage(), columns);
    }

    public static TableDesc generateKylinTableForMetricsJob(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsJob());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectJob(), columns);
    }

    public static TableDesc generateKylinTableForMetricsJobException(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsJobException());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectJobException(), columns);
    }

    public static TableDesc generateKylinTable(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc, String subject,
                                               List<Pair<String, String>> columns) {
        TableDesc kylinTable = new TableDesc();

        Pair<String, String> tableNameSplits = ActiveReservoirReporter
                .getTableNameSplits(sinkDesc.getTableNameForMetrics(subject));
        kylinTable.setUuid(RandomUtil.randomUUID().toString());
        kylinTable.setDatabase(tableNameSplits.getFirst());
        kylinTable.setName(tableNameSplits.getSecond());
        kylinTable.setTableType(null);
        kylinTable.setLastModified(0L);
        kylinTable.setSourceType(sinkDesc.getSourceType());

        ColumnDesc[] columnDescs = new ColumnDesc[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnDescs[i] = new ColumnDesc();
            Pair<String, String> entry = columns.get(i);
            columnDescs[i].setId(Integer.toString(i + 1));
            columnDescs[i].setName(entry.getFirst());
            columnDescs[i].setDatatype(entry.getSecond());
        }
        kylinTable.setColumns(columnDescs);

        kylinTable.init(kylinConfig, MetricsManager.SYSTEM_PROJECT);

        return kylinTable;
    }
}

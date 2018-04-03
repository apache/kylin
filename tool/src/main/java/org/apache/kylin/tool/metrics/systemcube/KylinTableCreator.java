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
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.metrics.lib.SinkTool;
import org.apache.kylin.tool.metrics.systemcube.util.HiveSinkTool;

import com.google.common.collect.Lists;

public class KylinTableCreator {

    public static void main(String[] args) throws Exception {
        //        KylinConfig.setSandboxEnvIfPossible();
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        TableDesc kylinTable = generateKylinTableForMetricsQuery(config, new HiveSinkTool());
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        TableMetadataManager.TABLE_SERIALIZER.serialize(kylinTable, dout);
        dout.close();
        buf.close();
        System.out.println(buf.toString());
    }

    public static TableDesc generateKylinTableForMetricsQuery(KylinConfig kylinConfig, SinkTool sinkTool) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQuery());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkTool, kylinConfig.getKylinMetricsSubjectQuery(), columns);
    }

    public static TableDesc generateKylinTableForMetricsQueryCube(KylinConfig kylinConfig, SinkTool sinkTool) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQueryCube());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkTool, kylinConfig.getKylinMetricsSubjectQueryCube(), columns);
    }

    public static TableDesc generateKylinTableForMetricsQueryRPC(KylinConfig kylinConfig, SinkTool sinkTool) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQueryRPC());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkTool, kylinConfig.getKylinMetricsSubjectQueryRpcCall(), columns);
    }

    public static TableDesc generateKylinTableForMetricsJob(KylinConfig kylinConfig, SinkTool sinkTool) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsJob());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkTool, kylinConfig.getKylinMetricsSubjectJob(), columns);
    }

    public static TableDesc generateKylinTableForMetricsJobException(KylinConfig kylinConfig, SinkTool sinkTool) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsJobException());
        columns.addAll(HiveTableCreator.getPartitionKVsForHiveTable());
        return generateKylinTable(kylinConfig, sinkTool, kylinConfig.getKylinMetricsSubjectJobException(), columns);
    }

    public static TableDesc generateKylinTable(KylinConfig kylinConfig, SinkTool sinkTool, String subject,
            List<Pair<String, String>> columns) {
        TableDesc kylinTable = new TableDesc();

        Pair<String, String> tableNameSplits = ActiveReservoirReporter
                .getTableNameSplits(sinkTool.getTableNameForMetrics(subject));
        kylinTable.setUuid(UUID.randomUUID().toString());
        kylinTable.setDatabase(tableNameSplits.getFirst());
        kylinTable.setName(tableNameSplits.getSecond());
        kylinTable.setTableType(null);
        kylinTable.setLastModified(0L);
        kylinTable.setSourceType(sinkTool.getSourceType());

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

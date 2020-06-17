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
package org.apache.kylin.tool.metrics.systemcube.streamingv2;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metrics.lib.impl.kafka.KafkaReservoirReporter;
import org.apache.kylin.stream.core.source.MessageParserInfo;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
import org.apache.kylin.tool.metrics.systemcube.HiveTableCreator;
import org.apache.kylin.tool.metrics.systemcube.def.MetricsSinkDesc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamingMetadataCreator {

    public static final Serializer<StreamingSourceConfig> STREAMING_SOURCE_CONFIG_SERIALIZER = new JsonSerializer<>(StreamingSourceConfig.class);


    public static StreamingSourceConfig generateKylinTableForMetricsQuery(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQuery());
        return generateStreamingV2Config(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectQuery(), columns);
    }

    public static StreamingSourceConfig generateKylinTableForMetricsQueryCube(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQueryCube());
        return generateStreamingV2Config(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectQueryCube(), columns);
    }

    public static StreamingSourceConfig generateKylinTableForMetricsQueryRpcCall(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsQueryRPC());
        return generateStreamingV2Config(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectQueryRpcCall(), columns);
    }

    public static StreamingSourceConfig generateKylinTableForMetricsJob(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsJob());
        return generateStreamingV2Config(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectJob(), columns);
    }

    public static StreamingSourceConfig generateKylinTableForMetricsJobException(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc) {
        List<Pair<String, String>> columns = Lists.newLinkedList();
        columns.addAll(HiveTableCreator.getHiveColumnsForMetricsJobException());
        return generateStreamingV2Config(kylinConfig, sinkDesc, kylinConfig.getKylinMetricsSubjectJobException(), columns);
    }

    private static StreamingSourceConfig generateStreamingV2Config(KylinConfig kylinConfig, MetricsSinkDesc sinkDesc, String subject, List<Pair<String, String>> columns) {
        StreamingSourceConfig streamingSourceConfig = new StreamingSourceConfig();
        MessageParserInfo parserInfo = new MessageParserInfo();
        parserInfo.setFormatTs(false);
        parserInfo.setTsColName("KTIMESTAMP");
        parserInfo.setTsPattern("MS");
        parserInfo.setTsParser("org.apache.kylin.stream.source.kafka.LongTimeParser");
        Map<String, String> columnToSourceFieldMapping = new HashMap<>();
        for (Pair<String, String> col : columns) {
            columnToSourceFieldMapping.put(col.getKey(), col.getKey());
        }
        parserInfo.setColumnToSourceFieldMapping(columnToSourceFieldMapping);

        Map<String, String> properties = new HashMap<>();
        String topic = KafkaReservoirReporter.decorateTopic(subject);
        String table = KafkaReservoirReporter.sink.getTableFromSubject(subject);

        properties.put("topic", topic);
        properties.put("bootstrap.servers", sinkDesc.getTableProperties().get("bootstrap.servers"));

        streamingSourceConfig.setName(table);
        streamingSourceConfig.setProperties(properties);
        streamingSourceConfig.setParserInfo(parserInfo);

        streamingSourceConfig.updateRandomUuid();
        streamingSourceConfig.setLastModified(System.currentTimeMillis());

        return streamingSourceConfig;
    }

}

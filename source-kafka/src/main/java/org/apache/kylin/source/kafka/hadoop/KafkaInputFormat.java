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

package org.apache.kylin.source.kafka.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kylin.source.kafka.config.KafkaConsumerProperties;
import org.apache.kylin.source.kafka.util.KafkaClient;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Convert Kafka topic to Hadoop InputFormat
 * Modified from the kafka-hadoop-loader in https://github.com/amient/kafka-hadoop-loader
 */
public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        final Configuration conf = context.getConfiguration();

        final String brokers = conf.get(KafkaFlatTableJob.CONFIG_KAFKA_BROKERS);
        final String inputTopic = conf.get(KafkaFlatTableJob.CONFIG_KAFKA_TOPIC);
        final String consumerGroup = conf.get(KafkaFlatTableJob.CONFIG_KAFKA_CONSUMER_GROUP);
        final Integer partitionMin = Integer.valueOf(conf.get(KafkaFlatTableJob.CONFIG_KAFKA_PARITION_MIN));
        final Integer partitionMax = Integer.valueOf(conf.get(KafkaFlatTableJob.CONFIG_KAFKA_PARITION_MAX));

        final Map<Integer, Long> startOffsetMap = Maps.newHashMap();
        final Map<Integer, Long> endOffsetMap = Maps.newHashMap();
        for (int i = partitionMin; i <= partitionMax; i++) {
            String start = conf.get(KafkaFlatTableJob.CONFIG_KAFKA_PARITION_START + i);
            String end = conf.get(KafkaFlatTableJob.CONFIG_KAFKA_PARITION_END + i);
            if (start != null && end != null) {
                startOffsetMap.put(i, Long.valueOf(start));
                endOffsetMap.put(i, Long.valueOf(end));
            }
        }

        Properties kafkaProperties = KafkaConsumerProperties.extractKafkaConfigToProperties(conf);
        final List<InputSplit> splits = new ArrayList<InputSplit>();
        try (KafkaConsumer<String, String> consumer = KafkaClient.getKafkaConsumer(brokers, consumerGroup, kafkaProperties)) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(inputTopic);
            Preconditions.checkArgument(partitionInfos.size() == startOffsetMap.size(), "partition number mismatch with server side");
            for (int i = 0; i < partitionInfos.size(); i++) {
                final PartitionInfo partition = partitionInfos.get(i);
                int partitionId = partition.partition();
                if (startOffsetMap.containsKey(partitionId) == false) {
                    throw new IllegalStateException("Partition '" + partitionId + "' not exists.");
                }

                if (endOffsetMap.get(partitionId) > startOffsetMap.get(partitionId)) {
                    InputSplit split = new KafkaInputSplit(brokers, inputTopic, partitionId, startOffsetMap.get(partitionId), endOffsetMap.get(partitionId));
                    splits.add(split);
                }
            }
        }
        return splits;
    }

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new KafkaInputRecordReader();
    }

}

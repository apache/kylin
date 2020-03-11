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

package org.apache.kylin.stream.source.kafka.consumer;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.stream.core.consumer.ConsumerStartMode;
import org.apache.kylin.stream.core.consumer.IStreamingConnector;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.source.IStreamingMessageParser;
import org.apache.kylin.stream.core.source.IStreamingSource;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.source.kafka.KafkaSource;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class KafkaConnector implements IStreamingConnector {
    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final String topic;
    private final IStreamingMessageParser parser;
    private ConsumerStartMode startMode = ConsumerStartMode.EARLIEST;

    private List<ConsumerRecord<byte[], byte[]>> buffer = Lists.newLinkedList();
    private List<Partition> partitions;
    private Map<Integer, Long> partitionOffsets;

    private KafkaSource kafkaSource;

    public KafkaConnector(Map<String, Object> conf, String topic, IStreamingMessageParser parser, KafkaSource kafkaSource) {
        this.kafkaConsumer = new KafkaConsumer<>(conf);
        this.topic = topic;
        this.parser = parser;
        this.kafkaSource = kafkaSource;
    }

    public void setStartPartition(List<Partition> partitions, ConsumerStartMode startMode,
            Map<Integer, Long> partitionOffsets) {
        this.partitions = partitions;
        this.startMode = startMode;
        this.partitionOffsets = partitionOffsets;
    }

    @Override
    public List<Partition> getConsumePartitions() {
        return partitions;
    }

    @Override
    public void open() {
        if (partitions == null || partitions.size() <= 0) {
            throw new IllegalStateException("not assign partitions");
        }
        List<TopicPartition> topicPartitions = Lists.newArrayList();
        for (Partition partition : partitions) {
            topicPartitions.add(new TopicPartition(topic, partition.getPartitionId()));
        }
        kafkaConsumer.assign(topicPartitions);

        if (startMode == ConsumerStartMode.EARLIEST) {
            kafkaConsumer.seekToBeginning(topicPartitions);
        } else if (startMode == ConsumerStartMode.LATEST) {
            kafkaConsumer.seekToEnd(topicPartitions);
        } else {
            for (TopicPartition topicPartition : topicPartitions) {
                Long offset = partitionOffsets.get(topicPartition.partition());
                kafkaConsumer.seek(topicPartition, offset);
            }
        }
    }

    @Override
    public void close() {
        kafkaConsumer.close();
    }

    @Override
    public void wakeup() {
        kafkaConsumer.wakeup();
    }

    @Override
    public StreamingMessage nextEvent() {
        if (buffer.isEmpty()) {
            fillBuffer();
        }
        if (buffer.isEmpty()) {
            return null;
        }
        ConsumerRecord<byte[], byte[]> record = buffer.remove(0);
        return parser.parse(record);
    }

    private void fillBuffer() {
        ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(100);
        List<ConsumerRecord<byte[], byte[]>> newBuffer = Lists.newLinkedList();
        for (TopicPartition topicPartition : records.partitions()) {
            newBuffer.addAll(records.records(topicPartition));
        }
        this.buffer = newBuffer;
    }

    public IStreamingSource getSource() {
        return kafkaSource;
    }
}

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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.source.kafka.config.KafkaConsumerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert Kafka topic to Hadoop InputFormat
 * Modified from the kafka-hadoop-loader in https://github.com/amient/kafka-hadoop-loader
 */
public class KafkaInputRecordReader extends RecordReader<LongWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(KafkaInputRecordReader.class);

    public static final long DEFAULT_KAFKA_CONSUMER_POLL_TIMEOUT = 60000;

    private Configuration conf;

    private KafkaInputSplit split;
    private Consumer consumer;
    private String brokers;
    private String topic;

    private int partition;
    private long earliestOffset;
    private long watermark;
    private long latestOffset;

    private ConsumerRecords<String, String> messages;
    private Iterator<ConsumerRecord<String, String>> iterator;
    private LongWritable key;
    private BytesWritable value;

    private long timeOut = DEFAULT_KAFKA_CONSUMER_POLL_TIMEOUT;

    private long numProcessedMessages = 0L;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException, InterruptedException {
        this.conf = conf;
        this.split = (KafkaInputSplit) split;
        brokers = this.split.getBrokers();
        topic = this.split.getTopic();
        partition = this.split.getPartition();
        watermark = this.split.getOffsetStart();

        if (conf.get(KafkaFlatTableJob.CONFIG_KAFKA_TIMEOUT) != null) {
            timeOut = Long.parseLong(conf.get(KafkaFlatTableJob.CONFIG_KAFKA_TIMEOUT));
        }
        String consumerGroup = conf.get(KafkaFlatTableJob.CONFIG_KAFKA_CONSUMER_GROUP);

        Properties kafkaProperties = KafkaConsumerProperties.extractKafkaConfigToProperties(conf);

        consumer = org.apache.kylin.source.kafka.util.KafkaClient.getKafkaConsumer(brokers, consumerGroup, kafkaProperties);

        earliestOffset = this.split.getOffsetStart();
        latestOffset = this.split.getOffsetEnd();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));
        log.info("Split {} Topic: {} Broker: {} Partition: {} Start: {} End: {}", new Object[] { this.split, topic, this.split.getBrokers(), partition, earliestOffset, latestOffset });
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable();
        }

        if (watermark >= latestOffset) {
            log.info("Reach the end offset, stop reading.");
            return false;
        }

        if (messages == null) {
            log.info("{} fetching offset {} ", topic + ":" + split.getBrokers() + ":" + partition, watermark);
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.seek(topicPartition, watermark);
            messages = consumer.poll(timeOut);
            iterator = messages.iterator();
            if (!iterator.hasNext()) {
                log.info("No more messages, stop");
                throw new IOException(String.format("Unexpected ending of stream, expected ending offset %d, but end at %d", latestOffset, watermark));
            }
        }

        if (iterator.hasNext()) {
            ConsumerRecord<String, String> message = iterator.next();
            key.set(message.offset());
            byte[] valuebytes = Bytes.toBytes(message.value());
            value.set(valuebytes, 0, valuebytes.length);
            watermark = message.offset() + 1;
            numProcessedMessages++;
            if (!iterator.hasNext()) {
                messages = null;
                iterator = null;
            }
            return true;
        }

        log.error("Unexpected iterator end.");
        throw new IOException(String.format("Unexpected ending of stream, expected ending offset %d, but end at %d", latestOffset, watermark));
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (watermark >= latestOffset || earliestOffset == latestOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (watermark - earliestOffset) / (float) (latestOffset - earliestOffset));
    }

    @Override
    public void close() throws IOException {
        log.info("{} num. processed messages {} ", topic + ":" + split.getBrokers() + ":" + partition, numProcessedMessages);
        consumer.close();
    }

}

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Convert Kafka topic to Hadoop InputFormat
 * Modified from the kafka-hadoop-loader in https://github.com/amient/kafka-hadoop-loader
 */
public class KafkaInputSplit extends InputSplit implements Writable {

    private String brokers;
    private String topic;
    private int partition;
    private long offsetStart;
    private long offsetEnd;

    public KafkaInputSplit() {
    }

    public KafkaInputSplit(String brokers, String topic, int partition, long offsetStart, long offsetEnd) {
        this.brokers = brokers;
        this.topic = topic;
        this.partition = partition;
        this.offsetStart = offsetStart;
        this.offsetEnd = offsetEnd;
    }

    public void readFields(DataInput in) throws IOException {
        brokers = Text.readString(in);
        topic = Text.readString(in);
        partition = in.readInt();
        offsetStart = in.readLong();
        offsetEnd = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, brokers);
        Text.writeString(out, topic);
        out.writeInt(partition);
        out.writeLong(offsetStart);
        out.writeLong(offsetEnd);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return Long.MAX_VALUE;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[]{brokers};
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokers() {
        return brokers;
    }

    public long getOffsetStart() {
        return offsetStart;
    }

    public long getOffsetEnd() {
        return offsetEnd;
    }

    @Override
    public String toString() {
        return brokers + "-" + topic + "-" + partition + "-" + offsetStart + "-" + offsetEnd;
    }
}
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

package org.apache.kylin.metrics.lib.impl.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.impl.ReporterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reporter which listens for new records and publishes them to Kafka.
 */
public class KafkaReservoirReporter extends ActiveReservoirReporter {

    public static final String KAFKA_REPORTER_SUFFIX = "KAFKA";
    public static final KafkaSink sink = new KafkaSink();
    protected static final Logger logger = LoggerFactory.getLogger(KafkaReservoirReporter.class);
    private final ActiveReservoir activeReservoir;
    private final KafkaReservoirListener listener;

    private KafkaReservoirReporter(ActiveReservoir activeReservoir, Properties props) {
        this.activeReservoir = activeReservoir;
        this.listener = new KafkaReservoirListener(props);
    }

    /**
     * Returns a new {@link Builder} for {@link KafkaReservoirReporter}.
     *
     * @param activeReservoir the registry to report
     * @return a {@link Builder} instance for a {@link KafkaReservoirReporter}
     */
    public static Builder forRegistry(ActiveReservoir activeReservoir) {
        return new Builder(activeReservoir);
    }

    public static String decorateTopic(String topic) {
        return ActiveReservoirReporter.KYLIN_PREFIX + "_" + KAFKA_REPORTER_SUFFIX + "_" + topic;
    }

    public static String getTableFromSubject(String subject) {
        return sink.getTableFromSubject(subject);
    }

    /**
     * Starts the reporter.
     */
    public void start() {
        activeReservoir.addListener(listener);
    }

    /**
     * Stops the reporter.
     */
    public void stop() {
        activeReservoir.removeListener(listener);
    }

    /**
     * Stops the reporter.
     */
    @Override
    public void close() {
        stop();
    }

    /**
     * A builder for {@link KafkaReservoirReporter} instances.
     */
    public static class Builder extends ReporterBuilder {

        private Builder(ActiveReservoir activeReservoir) {
            super(activeReservoir);
        }

        private void setFixedProperties() {
            props.put("key.serializer", ByteArraySerializer.class.getName());
            props.put("value.serializer", ByteArraySerializer.class.getName());
        }

        /**
         * Builds a {@link KafkaReservoirReporter} with the given properties.
         *
         * @return a {@link KafkaReservoirReporter}
         */
        public KafkaReservoirReporter build() {
            setFixedProperties();
            return new KafkaReservoirReporter(registry, props);
        }
    }

    private class KafkaReservoirListener extends KafkaActiveReserviorListener {
        protected final Producer<byte[], byte[]> producer;

        private KafkaReservoirListener(Properties props) {
            producer = new KafkaProducer<>(props);
        }

        public void tryFetchMetadataFor(String topic) {
            producer.partitionsFor(topic);
        }

        protected String decorateTopic(String topic) {
            return KafkaReservoirReporter.decorateTopic(topic);
        }

        protected void send(String topic, Record record, Callback callback) {
            producer.send(new ProducerRecord<>(topic, record.getKey(), record.getValue()), callback);
        }

        public void close() {
            producer.close();
        }
    }
}

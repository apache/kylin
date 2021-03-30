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

package org.apache.kylin.source.kafka.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A sample producer which will create sample data to kafka topic
 */
public class KafkaSampleProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSampleProducer.class);
    @SuppressWarnings("static-access")
    private static final Option OPTION_TOPIC = OptionBuilder.withArgName("topic").hasArg().isRequired(true).withDescription("Kafka topic").create("topic");
    private static final Option OPTION_BROKER = OptionBuilder.withArgName("broker").hasArg().isRequired(true).withDescription("Kafka broker").create("broker");
    private static final Option OPTION_INTERVAL = OptionBuilder.withArgName("interval").hasArg().isRequired(false).withDescription("Simulated message interval in mili-seconds, default 1000").create("interval");

    protected static final String OTHER = "Other";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if(logger.isInfoEnabled()) logger.info("args: {}", Arrays.toString(args));
        OptionsHelper optionsHelper = new OptionsHelper();
        Options options = new Options();
        options.addOption(OPTION_TOPIC);
        options.addOption(OPTION_BROKER);
        options.addOption(OPTION_INTERVAL);
        optionsHelper.parseOptions(options, args);

        logger.info("options: '{}'", optionsHelper.getOptionsAsString());

        final String topic = optionsHelper.getOptionValue(OPTION_TOPIC);
        final String broker = optionsHelper.getOptionValue(OPTION_BROKER);

        long interval = 10;
        String intervalString = optionsHelper.getOptionValue(OPTION_INTERVAL);
        if (intervalString != null) {
            interval = Long.parseLong(intervalString);
        }

        List<String> countries = new ArrayList<>();
        countries.add("AUSTRALIA");
        countries.add("CANADA");
        countries.add("CHINA");
        countries.add("INDIA");
        countries.add("JAPAN");
        countries.add("KOREA");
        countries.add("US");
        countries.add(OTHER);
        List<String> category = new ArrayList<>();
        category.add("BOOK");
        category.add("TOY");
        category.add("CLOTH");
        category.add("ELECTRONIC");
        category.add(OTHER);
        List<String> devices = new ArrayList<>();
        devices.add("iOS");
        devices.add("Windows");
        devices.add("Andriod");
        devices.add(OTHER);

        List<String> genders = new ArrayList<>();
        genders.add("Male");
        genders.add("Female");

        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        long startTime = System.currentTimeMillis();
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            boolean alive = true;
            Random rnd = new Random();
            Map<String, Object> record = new HashMap<>();
            while (alive) {
                //add normal record
                record.put("order_time", (new Date().getTime()));
                record.put("country", countries.get(rnd.nextInt(countries.size())));
                record.put("category", category.get(rnd.nextInt(category.size())));
                record.put("device", devices.get(rnd.nextInt(devices.size())));
                record.put("qty", rnd.nextInt(10));
                record.put("currency", "USD");
                record.put("amount", rnd.nextDouble() * 100);
                //add embedded record
                Map<String, Object> user = new HashMap<>();
                user.put("id", RandomUtil.randomUUID().toString());
                user.put("gender", genders.get(rnd.nextInt(2)));
                user.put("age", rnd.nextInt(20) + 10);
                user.put("first_name", "unknown");
                record.put("user", user);
                //send message
                ProducerRecord<String, String> data = new ProducerRecord<>(topic, System.currentTimeMillis() + "", mapper.writeValueAsString(record));
                if (logger.isInfoEnabled())
                    logger.info("Sending 1 message: {}", JsonUtil.writeValueAsString(record));
                producer.send(data);
                Thread.sleep(interval);
                if (System.currentTimeMillis() - startTime >= 7 * 24 * 3600 * 1000) {
                    alive = false;
                }
            }
        }
    }
}

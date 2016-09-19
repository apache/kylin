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
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * A sample producer which will create sample data to kafka topic
 */
public class KafkaSampleProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSampleProducer.class);
    @SuppressWarnings("static-access")
    private static final Option OPTION_TOPIC = OptionBuilder.withArgName("topic").hasArg().isRequired(true).withDescription("Kafka topic").create("topic");
    private static final Option OPTION_BROKER = OptionBuilder.withArgName("broker").hasArg().isRequired(true).withDescription("Kafka broker").create("broker");
    private static final Option OPTION_DELAY = OptionBuilder.withArgName("delay").hasArg().isRequired(false).withDescription("Simulated message delay").create("delay");

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        logger.info("args: " + Arrays.toString(args));
        OptionsHelper optionsHelper = new OptionsHelper();
        Options options = new Options();
        String topic, broker;
        options.addOption(OPTION_TOPIC);
        options.addOption(OPTION_BROKER);
        options.addOption(OPTION_DELAY);
        optionsHelper.parseOptions(options, args);

        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");

        topic = optionsHelper.getOptionValue(OPTION_TOPIC);
        broker = optionsHelper.getOptionValue(OPTION_BROKER);
        long delay = 0;
        String delayString = optionsHelper.getOptionValue(OPTION_DELAY);
        if (delayString != null) {
            delay = Long.parseLong(optionsHelper.getOptionValue(OPTION_DELAY));
        }

        List<String> countries = new ArrayList();
        countries.add("AUSTRALIA");
        countries.add("CANADA");
        countries.add("CHINA");
        countries.add("INDIA");
        countries.add("JAPAN");
        countries.add("KOREA");
        countries.add("US");
        countries.add("Other");
        List<String> category = new ArrayList();
        category.add("BOOK");
        category.add("TOY");
        category.add("CLOTH");
        category.add("ELECTRONIC");
        category.add("Other");
        List<String> devices = new ArrayList();
        devices.add("iOS");
        devices.add("Windows");
        devices.add("Andriod");
        devices.add("Other");

        Properties props = new Properties();
        props.put("metadata.broker.list", broker);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        boolean alive = true;
        Random rnd = new Random();
        Map<String, Object> record = new HashMap();
        while (alive == true) {
            record.put("order_time", (new Date().getTime() - delay));
            record.put("country", countries.get(rnd.nextInt(countries.size())));
            record.put("category", category.get(rnd.nextInt(category.size())));
            record.put("device", devices.get(rnd.nextInt(devices.size())));
            record.put("qty", rnd.nextInt(10));
            record.put("currency", "USD");
            record.put("amount", rnd.nextDouble() * 100);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, System.currentTimeMillis() + "", mapper.writeValueAsString(record));
            System.out.println("Sending 1 message");
            producer.send(data);
            Thread.sleep(2000);
        }
        producer.close();
    }

}

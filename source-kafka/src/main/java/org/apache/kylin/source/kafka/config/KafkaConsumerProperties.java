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

package org.apache.kylin.source.kafka.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigCannotInitException;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerProperties {

    public static final String KAFKA_CONSUMER_FILE = "kylin-kafka-consumer.xml";
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerProperties.class);
    // static cached instances
    private static KafkaConsumerProperties ENV_INSTANCE = null;
    private volatile Properties properties = new Properties();

    private KafkaConsumerProperties() {

    }

    public static KafkaConsumerProperties getInstanceFromEnv() {
        synchronized (KafkaConsumerProperties.class) {
            if (ENV_INSTANCE == null) {
                try {
                    KafkaConsumerProperties config = new KafkaConsumerProperties();
                    config.properties = config.loadKafkaConsumerProperties();

                    logger.info("Initialized a new KafkaConsumerProperties from getInstanceFromEnv : " + System.identityHashCode(config));
                    ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KafkaConsumerProperties ", e);
                }
            }
            return ENV_INSTANCE;
        }
    }

    private static File getKafkaConsumerFile(String path) {
        if (path == null) {
            return null;
        }

        return new File(path, KAFKA_CONSUMER_FILE);
    }

    public static Properties extractKafkaConfigToProperties(Configuration configuration) {
        Set<String> configNames = new HashSet<String>();
        try {
            configNames = ConsumerConfig.configNames();
        } catch (Error e) {
            // the Kafka configNames api is supported on 0.10.1.0+, in case NoSuchMethodException which is an Error, not Exception
            String[] configNamesArray = ("metric.reporters, metadata.max.age.ms, partition.assignment.strategy, reconnect.backoff.ms," + "sasl.kerberos.ticket.renew.window.factor, max.partition.fetch.bytes, bootstrap.servers, ssl.keystore.type," + " enable.auto.commit, sasl.mechanism, interceptor.classes, exclude.internal.topics, ssl.truststore.password," + " client.id, ssl.endpoint.identification.algorithm, max.poll.records, check.crcs, request.timeout.ms, heartbeat.interval.ms," + " auto.commit.interval.ms, receive.buffer.bytes, ssl.truststore.type, ssl.truststore.location, ssl.keystore.password, fetch.min.bytes," + " fetch.max.bytes, send.buffer.bytes, max.poll.interval.ms, value.deserializer, group.id, retry.backoff.ms,"
                    + " ssl.secure.random.implementation, sasl.kerberos.kinit.cmd, sasl.kerberos.service.name, sasl.kerberos.ticket.renew.jitter, ssl.trustmanager.algorithm, ssl.key.password, fetch.max.wait.ms, sasl.kerberos.min.time.before.relogin, connections.max.idle.ms, session.timeout.ms, metrics.num.samples, key.deserializer, ssl.protocol, ssl.provider, ssl.enabled.protocols, ssl.keystore.location, ssl.cipher.suites, security.protocol, ssl.keymanager.algorithm, metrics.sample.window.ms, auto.offset.reset").split(",");
            configNames.addAll(Arrays.asList(configNamesArray));
        }

        Properties result = new Properties();
        for (Iterator<Map.Entry<String, String>> it = configuration.iterator(); it.hasNext();) {
            Map.Entry<String, String> entry = it.next();
            String key = entry.getKey();
            String value = entry.getValue();
            if (configNames.contains(key)) {
                result.put(key, value);
            }
        }
        return result;
    }

    private Properties loadKafkaConsumerProperties() {
        File propFile = getKafkaConsumerFile();
        if (propFile == null || !propFile.exists()) {
            logger.warn("fail to locate " + KAFKA_CONSUMER_FILE + ", use empty kafka consumer properties");
            return new Properties();
        }
        Properties properties = new Properties();
        try {
            FileInputStream is = new FileInputStream(propFile);
            Configuration conf = new Configuration();
            conf.addResource(is);
            properties.putAll(extractKafkaConfigToProperties(conf));
            IOUtils.closeQuietly(is);

            File propOverrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
            if (propOverrideFile.exists()) {
                FileInputStream ois = new FileInputStream(propOverrideFile);
                Configuration oconf = new Configuration();
                oconf.addResource(ois);
                properties.putAll(extractKafkaConfigToProperties(oconf));
                IOUtils.closeQuietly(ois);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return properties;
    }

    public String getKafkaConsumerHadoopJobConf() {
        File kafkaConsumerFile = getKafkaConsumerFile();
        return OptionsHelper.convertToFileURL(kafkaConsumerFile.getAbsolutePath());
    }

    private File getKafkaConsumerFile() {
        String kylinConfHome = System.getProperty(KylinConfig.KYLIN_CONF);
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF=" + kylinConfHome);
            return getKafkaConsumerFile(kylinConfHome);
        }

        logger.warn("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = KylinConfig.getKylinHome();
        if (StringUtils.isEmpty(kylinHome))
            throw new KylinConfigCannotInitException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");

        String path = kylinHome + File.separator + "conf";
        return getKafkaConsumerFile(path);
    }

    public Properties extractKafkaConfigToProperties() {
        Properties prop = new Properties();
        prop.putAll(this.properties);
        return prop;
    }
}

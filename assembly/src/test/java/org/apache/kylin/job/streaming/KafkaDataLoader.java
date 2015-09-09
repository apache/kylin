package org.apache.kylin.job.streaming;

import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.kylin.source.kafka.config.BrokerConfig;
import org.apache.kylin.source.kafka.config.KafkaClusterConfig;

/**
 * Load prepared data into kafka(for test use)
 */
public class KafkaDataLoader {

    public static void loadIntoKafka(List<KafkaClusterConfig> kafkaClusterConfigs, List<String> messages) {

        KafkaClusterConfig clusterConfig = kafkaClusterConfigs.get(0);
        String brokerList = StringUtils.join(Collections2.transform(clusterConfig.getBrokerConfigs(), new Function<BrokerConfig, String>() {
            @Nullable
            @Override
            public String apply(BrokerConfig brokerConfig) {
                return brokerConfig.getHost() + ":" + brokerConfig.getPort();
            }
        }), ",");
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        List<KeyedMessage<String, String>> keyedMessages = Lists.newArrayList();
        for (int i = 0; i < messages.size(); ++i) {
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(clusterConfig.getTopic(), String.valueOf(i), messages.get(i));
            keyedMessages.add(keyedMessage);
        }
        producer.send(keyedMessages);
        producer.close();
    }

}

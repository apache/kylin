package org.apache.kylin.job.streaming;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.streaming.BrokerConfig;
import org.apache.kylin.streaming.KafkaClusterConfig;
import org.apache.kylin.streaming.StreamingConfig;
import org.apache.kylin.streaming.StreamingManager;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Load prepared data into kafka(for test use)
 */
public class KafkaDataLoader {

    public static void loadIntoKafka(String streamName, List<String> messages) {

        StreamingManager streamingManager = StreamingManager.getInstance(KylinConfig.getInstanceFromEnv());
        StreamingConfig streamingConfig = streamingManager.getStreamingConfig(streamName);

        KafkaClusterConfig clusterConfig = streamingConfig.getKafkaClusterConfigs().get(0);
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

        for (int i = 0; i < messages.size(); ++i) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(streamingConfig.getTopic(), String.valueOf(i), messages.get(i));
            producer.send(data);
        }
        producer.close();
    }

    /**
     *
     * @param args args[0] data file path, args[1] streaming name
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        List<String> alldata = FileUtils.readLines(new File(args[0]));
        loadIntoKafka(args[1], alldata);
    }
}

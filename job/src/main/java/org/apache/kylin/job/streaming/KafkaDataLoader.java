package org.apache.kylin.job.streaming;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.streaming.StreamingConfig;
import org.apache.kylin.streaming.StreamingManager;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 */
public class KafkaDataLoader {
    /**
     *
     * @param args args[0] data file path, args[1] streaming name
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        StreamingManager streamingManager = StreamingManager.getInstance(KylinConfig.getInstanceFromEnv());
        StreamingConfig streamingConfig = streamingManager.getStreamingConfig(args[1]);

        List<String> alldata = FileUtils.readLines(new File(args[0]));

        Properties props = new Properties();
        props.put("metadata.broker.list", "sandbox:6667");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        for (int i = 0; i < alldata.size(); ++i) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(streamingConfig.getTopic(), String.valueOf(i), alldata.get(i));
            producer.send(data);
        }
        producer.close();
    }
}

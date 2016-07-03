package org.apache.kylin.job.streaming;

import org.apache.kylin.source.kafka.config.KafkaClusterConfig;
import org.apache.kylin.source.kafka.config.KafkaConfig;

import java.util.List;

/**
 */
public abstract class StreamDataLoader {
    protected KafkaConfig kafkaConfig;
    public StreamDataLoader(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    abstract public void loadIntoKafka(List<String> messages);

    @Override
    public String toString() {
        return "kafka topic " + kafkaConfig.getTopic();
    }
}

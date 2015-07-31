package org.apache.kylin.source.kafka.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import kafka.cluster.Broker;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class KafkaClusterConfig extends RootPersistentEntity {
    public static Serializer<KafkaClusterConfig> SERIALIZER = new JsonSerializer<KafkaClusterConfig>(KafkaClusterConfig.class);

    @JsonProperty("brokers")
    private List<BrokerConfig> brokerConfigs;

    @JsonBackReference
    private StreamingConfig streamingConfig;

    public int getBufferSize() {
        return streamingConfig.getBufferSize();
    }

    public String getTopic() {
        return streamingConfig.getTopic();
    }

    public int getTimeout() {
        return streamingConfig.getTimeout();
    }

    public int getMaxReadCount() {
        return streamingConfig.getMaxReadCount();
    }

    public List<BrokerConfig> getBrokerConfigs() {
        return brokerConfigs;
    }

    public List<Broker> getBrokers() {
        return Lists.transform(brokerConfigs, new Function<BrokerConfig, Broker>() {
            @Nullable
            @Override
            public Broker apply(BrokerConfig input) {
                return new Broker(input.getId(), input.getHost(), input.getPort());
            }
        });
    }
}

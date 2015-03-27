package org.apache.kylin.streaming;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Created by Hongbin Ma(Binmahone) on 3/16/15.
 */
public class EternalStreamProducer {

    private static final Logger logger = LoggerFactory.getLogger(EternalStreamProducer.class);

    private int frequency;
    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);

    /**
     * @param frequency records added per second, 100 for recommendation
     */
    public EternalStreamProducer(int frequency) {
        if (frequency > 1000) {
            logger.warn("max EternalStreamProducer frequency is 1000, changed from {} to 1000", frequency);
            frequency = 1000;
        }

        if (frequency <= 0) {
            logger.warn("EternalStreamProducer frequency must be greater than 0, changed from {} to 1", frequency);
            frequency = 1;

        }
        this.frequency = frequency;
    }

    public void start() throws IOException, InterruptedException {
        final KafkaConfig kafkaConfig = StreamManager.getInstance(KylinConfig.getInstanceFromEnv()).getKafkaConfig("kafka_test");

        Properties props = new Properties();
        props.put("metadata.broker.list", StringUtils.join(Iterators.transform(kafkaConfig.getBrokers().iterator(), new Function<Broker, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Broker broker) {
                return broker.getConnectionString();
            }
        }), ","));
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        final Producer<String, String> producer = new Producer<String, String>(config);

        scheduledExecutorService.scheduleAtFixedRate(new Thread(new Runnable() {
            @Override
            public void run() {
                final KeyedMessage<String, String> message = new KeyedMessage<String, String>(kafkaConfig.getTopic(), getOneMessage());
                producer.send(message);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }), 0, 1000 / frequency, TimeUnit.MILLISECONDS);

    }

    public void stop() {
        scheduledExecutorService.shutdown();
    }

    protected String getOneMessage() {
        return "current time is:" + System.currentTimeMillis();
    }
}

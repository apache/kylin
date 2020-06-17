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

package org.apache.kylin.stream.source.kafka;

import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.stream.core.consumer.ConsumerStartMode;
import org.apache.kylin.stream.core.consumer.ConsumerStartProtocol;
import org.apache.kylin.stream.core.consumer.IStreamingConnector;
import org.apache.kylin.stream.core.exception.StreamingException;
import org.apache.kylin.stream.core.source.ISourcePosition;
import org.apache.kylin.stream.core.source.ISourcePositionHandler;
import org.apache.kylin.stream.core.source.IStreamingMessageParser;
import org.apache.kylin.stream.core.source.IStreamingSource;
import org.apache.kylin.stream.core.source.MessageParserInfo;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
import org.apache.kylin.stream.core.source.StreamingSourceConfigManager;
import org.apache.kylin.stream.core.source.StreamingSourceFactory;
import org.apache.kylin.stream.core.source.StreamingTableSourceInfo;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.source.kafka.consumer.KafkaConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Function;
import org.apache.kylin.shaded.com.google.common.collect.FluentIterable;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.MapDifference;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class KafkaSource implements IStreamingSource {
    public static final String PROP_TOPIC = "topic";
    public static final String PROP_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String PROP_MESSAGE_PARSER = "message.parser";
    private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);
    private static final String DEF_MSSAGE_PARSER_CLAZZ = "org.apache.kylin.stream.source.kafka.TimedJsonStreamParser";


    @Override
    public StreamingTableSourceInfo load(String cubeName) {
        KylinConfig kylinConf = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(kylinConf).getCube(cubeName);
        String streamingTableName = cube.getRootFactTable();
        StreamingSourceConfig streamingSourceConfig = StreamingSourceConfigManager.getInstance(kylinConf)
                .getConfig(streamingTableName);

        String topicName = getTopicName(streamingSourceConfig.getProperties());
        Map<String, Object> conf = getKafkaConf(streamingSourceConfig.getProperties(), cube.getConfig());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(conf);
        try {
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName);
            List<Partition> kafkaPartitions = Lists.transform(partitionInfos, new Function<PartitionInfo, Partition>() {
                @Nullable
                @Override
                public Partition apply(@Nullable PartitionInfo input) {
                    return new Partition(input.partition());
                }
            });
            return new StreamingTableSourceInfo(kafkaPartitions);
        } finally {
            kafkaConsumer.close();
        }
    }

    @Override
    public String getMessageTemplate(StreamingSourceConfig streamingSourceConfig) {
        String template = null;
        KafkaConsumer<byte[], byte[]> consumer = null;
        try {
            String topicName = getTopicName(streamingSourceConfig.getProperties());
            Map<String, Object> config = getKafkaConf(streamingSourceConfig.getProperties());
            consumer = new KafkaConsumer<>(config);
            Set<TopicPartition> partitions = Sets.newHashSet(FluentIterable.from(consumer.partitionsFor(topicName))
                    .transform(new Function<PartitionInfo, TopicPartition>() {
                        @Override
                        public TopicPartition apply(PartitionInfo input) {
                            return new TopicPartition(input.topic(), input.partition());
                        }
                    }));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            ConsumerRecords<byte[], byte[]> records = consumer.poll(500);
            if (records == null) {
                return null;
            }
            Iterator<ConsumerRecord<byte[], byte[]>> iterator = records.iterator();
            if (iterator == null || !iterator.hasNext()) {
                return null;
            }
            ConsumerRecord<byte[], byte[]> record = iterator.next();
            template = new String(record.value(), "UTF8");
        } catch (Exception e) {
            logger.error("error when fetch one record from kafka, stream:" + streamingSourceConfig.getName(), e);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return template;
    }

    @Override
    public IStreamingConnector createStreamingConnector(String cubeName, List<Partition> assignedPartitions,
            ConsumerStartProtocol startProtocol, StreamingSegmentManager streamingSegmentManager) {
        logger.info("Create StreamingConnector for Cube {}, assignedPartitions {}, startProtocol {}", cubeName,
                assignedPartitions, startProtocol);
        try {
            KylinConfig kylinConf = KylinConfig.getInstanceFromEnv();
            CubeInstance cubeInstance = CubeManager.getInstance(kylinConf).getCube(cubeName);
            IStreamingSource streamingSource = StreamingSourceFactory.getStreamingSource(cubeInstance);
            String streamingName = cubeInstance.getRootFactTable();
            StreamingSourceConfig streamingSourceConfig = StreamingSourceConfigManager.getInstance(kylinConf)
                    .getConfig(streamingName);
            String topic = getTopicName(streamingSourceConfig.getProperties());
            Map<String, Object> conf = getKafkaConf(streamingSourceConfig.getProperties(), cubeInstance.getConfig());

            Class<?> clazz = getStreamingMessageParserClass(streamingSourceConfig.getProperties());
            Constructor<?> constructor = clazz.getConstructor(CubeDesc.class, MessageParserInfo.class);
            IStreamingMessageParser<?> parser = (IStreamingMessageParser<?>) constructor
                    .newInstance(cubeInstance.getDescriptor(), streamingSourceConfig.getParserInfo());
            KafkaConnector connector = new KafkaConnector(conf, topic, parser, this);
            if (startProtocol != null) {
                if (startProtocol.getStartPosition() != null && startProtocol.getStartPosition().length() > 0) {
                    KafkaPosition position = (KafkaPosition) streamingSource.getSourcePositionHandler().parsePosition(startProtocol.getStartPosition());
                    connector.setStartPartition(assignedPartitions, startProtocol.getStartMode(),
                            position.getPartitionOffsets());
                    streamingSegmentManager.restoreConsumerStates(position);
                } else {
                    connector.setStartPartition(assignedPartitions, startProtocol.getStartMode(),
                            null);
                }
                streamingSegmentManager.checkpoint();
            } else if (streamingSegmentManager != null) {
                setupConnectorFromCheckpoint(connector, assignedPartitions, streamingSource, streamingSegmentManager);
            }

            return connector;
        } catch (Exception e) {
            throw new StreamingException("streaming connector create fail, cube:" + cubeName, e);
        }
    }

    @Override
    public ISourcePositionHandler getSourcePositionHandler() {
        return new KafkaPositionHandler();
    }

    private void setupConnectorFromCheckpoint(KafkaConnector connector, List<Partition> assignedPartitions, IStreamingSource streamingSource, StreamingSegmentManager cubeDataStore) {
        CubeInstance cubeInstance = cubeDataStore.getCubeInstance();
        CubeSegment latestReadySegment = cubeInstance.getLatestReadySegment();
        String localCheckpointConsumePos = cubeDataStore.getCheckPointSourcePosition();
        String remoteCheckpointConsumePos = null;
        if (latestReadySegment != null) {
            remoteCheckpointConsumePos = latestReadySegment.getStreamSourceCheckpoint();
        }
        logger.info("localConsumeStats from local checkpoint {}, remoteConsumeStats from remote checkpoint {} ",
                localCheckpointConsumePos, remoteCheckpointConsumePos);
        KafkaPosition localCPPosition = null;
        KafkaPosition remoteCPPosition = null;
        if (localCheckpointConsumePos != null) {
            localCPPosition = (KafkaPosition) streamingSource.getSourcePositionHandler().parsePosition(localCheckpointConsumePos);
        }

        if (remoteCheckpointConsumePos != null) {
            remoteCPPosition = (KafkaPosition) streamingSource.getSourcePositionHandler().parsePosition(remoteCheckpointConsumePos);
        }

        // merge the local and remote consume stats
        if (isEmptyPosition(localCPPosition) && isEmptyPosition(remoteCPPosition)) {
            // no segment exists in the cube and is configured to consume from latest offset
            if (cubeInstance.getSegments().isEmpty() && cubeInstance.getConfig().isStreamingConsumeFromLatestOffsets()) {
                logger.info("start kafka connector from latest");
                connector.setStartPartition(assignedPartitions, ConsumerStartMode.LATEST, null);
            } else {
                logger.info("start kafka connector from earliest");
                connector.setStartPartition(assignedPartitions, ConsumerStartMode.EARLIEST, null);
            }
            return;
        }

        KafkaPosition consumerStartPos;

        if (isEmptyPosition(localCPPosition) && !isEmptyPosition(remoteCPPosition)) {
            consumerStartPos = remoteCPPosition;
        } else if (isEmptyPosition(remoteCPPosition) && !isEmptyPosition(localCPPosition)) {
            consumerStartPos = (KafkaPosition)localCPPosition.advance();
        } else {
            Map<Integer, Long> mergedStartOffsets = Maps.newHashMap();
            MapDifference<Integer, Long> statsDiff = Maps.difference(localCPPosition.getPartitionOffsets(), remoteCPPosition.getPartitionOffsets());
            mergedStartOffsets.putAll(statsDiff.entriesInCommon());
            mergedStartOffsets.putAll(statsDiff.entriesOnlyOnLeft());
            mergedStartOffsets.putAll(statsDiff.entriesOnlyOnRight());
            mergedStartOffsets.putAll(Maps.transformValues(statsDiff.entriesDiffering(),
                    new Function<MapDifference.ValueDifference<Long>, Long>() {
                        @Nullable
                        @Override
                        public Long apply(@Nullable MapDifference.ValueDifference<Long> input) {
                            return input.leftValue() > input.rightValue() ? input.leftValue() : input.rightValue();
                        }
                    }));
            consumerStartPos = new KafkaPosition(mergedStartOffsets);
        }
        logger.info("start kafka connector from specified position:{}", consumerStartPos);
        connector.setStartPartition(assignedPartitions, ConsumerStartMode.SPECIFIC_POSITION, consumerStartPos.getPartitionOffsets());
    }

    private boolean isEmptyPosition(KafkaPosition kafkaPosition) {
        return kafkaPosition == null || kafkaPosition.getPartitionOffsets().isEmpty();
    }

    public static Map<String, Object> getKafkaConf(Map<String, String> sourceProperties, KylinConfig kylinConfig) {
        Map<String, String> kafkaConfigOverride = kylinConfig.getKafkaConfigOverride();
        Map<String, Object> kafkaConf = getKafkaConf(sourceProperties);
        kafkaConf.putAll(kafkaConfigOverride);

        return kafkaConf;
        //return getKafkaConf(sourceProperties);
    }

    public static Map<String, Object> getKafkaConf(Map<String, String> sourceProperties) {
        Map<String, Object> conf = Maps.newHashMap();
        String bootstrapServersString = getBootstrapServers(sourceProperties);
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersString);
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        conf.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(20000));
        conf.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(30000));
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        return conf;
    }

    public static String getBootstrapServers(Map<String, String> sourceProperties) {
        return sourceProperties.get(PROP_BOOTSTRAP_SERVERS);
    }

    public static String getTopicName(Map<String, String> sourceProperties) {
        return sourceProperties.get(PROP_TOPIC);
    }

    public static Class<?> getStreamingMessageParserClass(Map<String, String> sourceProperties)
            throws ClassNotFoundException {
        String parserName = sourceProperties.get(PROP_MESSAGE_PARSER);
        String parserClazzName = DEF_MSSAGE_PARSER_CLAZZ;
        if (parserName != null) {
            parserClazzName = getParserClassName(parserName);
        }
        return Class.forName(parserClazzName);
    }

    public static String getParserClassName(String parser) {
        return parser;
    }

    private ISourcePosition getLatestPosition(String cubeName) {
        KylinConfig kylinConf = KylinConfig.getInstanceFromEnv();
        CubeInstance cube = CubeManager.getInstance(kylinConf).getCube(cubeName);
        String streamingTableName = cube.getRootFactTable();
        StreamingSourceConfig streamingSourceConfig = StreamingSourceConfigManager.getInstance(kylinConf)
                .getConfig(streamingTableName);
        String topicName = getTopicName(streamingSourceConfig.getProperties());
        ISourcePosition sourcePosition = new KafkaPosition();
        Map<String, Object> conf = getKafkaConf(streamingSourceConfig.getProperties(), cube.getConfig());
        try (KafkaConsumer consumer = new KafkaConsumer<>(conf);) {
            Set<TopicPartition> partitions = Sets.newHashSet(FluentIterable.from(consumer.partitionsFor(topicName))
                    .transform(new Function<PartitionInfo, TopicPartition>() {
                        @Override
                        public TopicPartition apply(PartitionInfo input) {
                            return new TopicPartition(input.topic(), input.partition());
                        }
                    }));
            Map<TopicPartition, Long> lastestEventOffset = consumer.endOffsets(partitions);
            for (Map.Entry<TopicPartition, Long> entry : lastestEventOffset.entrySet()) {
                KafkaPosition.KafkaPartitionPosition partitionPosition = new KafkaPosition.KafkaPartitionPosition(
                        entry.getKey().partition(), entry.getValue());
                sourcePosition.update(partitionPosition);
            }
        }
        return sourcePosition;
    }

    public Map<Integer, Long> calConsumeLag(String cubeName, ISourcePosition currentPosition) {
        ISourcePosition latestPosition = getLatestPosition(cubeName);
        Map<Integer, Long> consumeLag = Maps.newHashMap();
        for (ISourcePosition.IPartitionPosition partitionPosition : currentPosition.getPartitionPositions().values()) {
            long current = ((KafkaPosition.KafkaPartitionPosition) partitionPosition).offset;
            ISourcePosition.IPartitionPosition latestPartition = latestPosition.getPartitionPositions()
                    .get(partitionPosition.getPartition());
            if (latestPartition != null) {
                long diff = ((KafkaPosition.KafkaPartitionPosition) latestPartition).offset - current;
                consumeLag.put(partitionPosition.getPartition(), diff);
            }
        }
        return consumeLag;
    }
}

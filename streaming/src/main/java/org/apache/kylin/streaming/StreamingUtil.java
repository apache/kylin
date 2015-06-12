package org.apache.kylin.streaming;

import com.google.common.base.Preconditions;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 */
public final class StreamingUtil {

    private static final Logger logger = LoggerFactory.getLogger(StreamingUtil.class);

    private StreamingUtil() {
    }

    public static Broker getLeadBroker(KafkaClusterConfig kafkaClusterConfig, int partitionId) {
        final PartitionMetadata partitionMetadata = KafkaRequester.getPartitionMetadata(kafkaClusterConfig.getTopic(), partitionId, kafkaClusterConfig.getBrokers(), kafkaClusterConfig);
        if (partitionMetadata != null && partitionMetadata.errorCode() == 0) {
            return partitionMetadata.leader();
        } else {
            return null;
        }
    }

    private static MessageAndOffset getKafkaMessage(KafkaClusterConfig kafkaClusterConfig, int partitionId, long offset) {
        final String topic = kafkaClusterConfig.getTopic();
        int retry = 3;
        while (retry > 0) {
            final Broker leadBroker = Preconditions.checkNotNull(getLeadBroker(kafkaClusterConfig, partitionId), "unable to find leadBroker with config:" + kafkaClusterConfig + " partitionId:" + partitionId);
            final FetchResponse response = KafkaRequester.fetchResponse(topic, partitionId, offset, leadBroker, kafkaClusterConfig);
            Preconditions.checkArgument(response.errorCode(topic, partitionId) == 0, "errorCode of FetchResponse is:" + response.errorCode(topic, partitionId));
            final Iterator<MessageAndOffset> iterator = response.messageSet(topic, partitionId).iterator();
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                retry--;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }
        }
        throw new IllegalStateException(String.format("try to get timestamp of topic: %s, partitionId: %d, offset: %d, failed to get StreamMessage from kafka", topic, partitionId, offset));
    }

    public static long getDataTimestamp(KafkaClusterConfig kafkaClusterConfig, int partitionId, long offset, StreamParser streamParser) {
        final String topic = kafkaClusterConfig.getTopic();
        logger.debug(String.format("try to get timestamp of topic: %s, partitionId: %d, offset: %d", topic, partitionId, offset));
        final MessageAndOffset messageAndOffset = getKafkaMessage(kafkaClusterConfig, partitionId, offset);
        final ByteBuffer payload = messageAndOffset.message().payload();
        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        final ParsedStreamMessage parsedStreamMessage = streamParser.parse(new StreamMessage(messageAndOffset.offset(), bytes));
        logger.debug(String.format("get timestamp:%d", parsedStreamMessage.getTimestamp()));
        return parsedStreamMessage.getTimestamp();
    }

    public static long findClosestOffsetWithDataTimestamp(KafkaClusterConfig kafkaClusterConfig, int partitionId, long timestamp, StreamParser streamParser) {
        final String topic = kafkaClusterConfig.getTopic();
        final Broker leadBroker = Preconditions.checkNotNull(getLeadBroker(kafkaClusterConfig, partitionId), "unable to find leadBroker with config:" + kafkaClusterConfig + " partitionId:" + partitionId);
        final long earliestOffset = KafkaRequester.getLastOffset(topic, partitionId, OffsetRequest.EarliestTime(), leadBroker, kafkaClusterConfig);
        final long latestOffset = KafkaRequester.getLastOffset(topic, partitionId, OffsetRequest.LatestTime(), leadBroker, kafkaClusterConfig);
        logger.info(String.format("topic: %s, partitionId: %d, try to find closest offset with timestamp: %d between offset {%d, %d}", topic, partitionId, timestamp, earliestOffset, latestOffset));
        final long result = binarySearch(kafkaClusterConfig, partitionId, earliestOffset, latestOffset, timestamp, streamParser);
        logger.info(String.format("topic: %s, partitionId: %d, found offset: %d", topic, partitionId, result));
        return result;
    }

    private static long binarySearch(KafkaClusterConfig kafkaClusterConfig, int partitionId, long startOffset, long endOffset, long targetTimestamp, StreamParser streamParser) {
        while (startOffset < endOffset) {
            long midOffset = startOffset + ((endOffset - startOffset) >> 1);
            long startTimestamp = getDataTimestamp(kafkaClusterConfig, partitionId, startOffset, streamParser);
            long endTimestamp = getDataTimestamp(kafkaClusterConfig, partitionId, endOffset, streamParser);
            long midTimestamp = getDataTimestamp(kafkaClusterConfig, partitionId, midOffset, streamParser);
            // hard to ensure these 2 conditions
//            Preconditions.checkArgument(startTimestamp <= midTimestamp);
//            Preconditions.checkArgument(midTimestamp <= endTimestamp);
            if (startTimestamp >= targetTimestamp) {
                return startOffset;
            }
            if (endTimestamp <= targetTimestamp) {
                return endOffset;
            }
            if (targetTimestamp == midTimestamp) {
                return midOffset;
            } else if (targetTimestamp < midTimestamp) {
                endOffset = midOffset - 1;
                continue;
            } else {
                startOffset = midOffset + 1;
                continue;
            }
        }
        return startOffset;


    }
}

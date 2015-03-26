/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.streaming;

import com.google.common.base.Preconditions;
import kafka.api.OffsetRequest;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.invertedindex.IIDescManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.streaming.invertedindex.IIStreamBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by qianzhou on 3/26/15.
 */
public class StreamingBootstrap {

    private static KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
    private static StreamManager streamManager = StreamManager.getInstance(kylinConfig);
    private static IIManager iiManager = IIManager.getInstance(kylinConfig);
    private static IIDescManager iiDescManager = IIDescManager.getInstance(kylinConfig);


    private static Broker getLeadBroker(KafkaConfig kafkaConfig, int partitionId) {
        final PartitionMetadata partitionMetadata = KafkaRequester.getPartitionMetadata(kafkaConfig.getTopic(), partitionId, kafkaConfig.getBrokers(), kafkaConfig);
        if (partitionMetadata != null && partitionMetadata.errorCode() == 0) {
            return partitionMetadata.leader();
        } else {
            return null;
        }
    }

    public static void startStreaming(String streamingConf, int partitionId) throws Exception {
        final KafkaConfig kafkaConfig = streamManager.getKafkaConfig(streamingConf);
        Preconditions.checkArgument(kafkaConfig != null, "cannot find kafka config:" + streamingConf);
        final IIInstance ii = iiManager.getII(kafkaConfig.getIiName());
        Preconditions.checkNotNull(ii);

        final Broker leadBroker = getLeadBroker(kafkaConfig, partitionId);
        Preconditions.checkState(leadBroker != null, "cannot find lead broker");
        final long earliestOffset = KafkaRequester.getLastOffset(kafkaConfig.getTopic(), partitionId, OffsetRequest.EarliestTime(), leadBroker, kafkaConfig);
        long streamOffset = ii.getStreamOffsets().get(partitionId);
        if (streamOffset < earliestOffset) {
            streamOffset = earliestOffset;
        }


        KafkaConsumer consumer = new KafkaConsumer(kafkaConfig.getTopic(), 0, streamOffset, kafkaConfig.getBrokers(), kafkaConfig) {
            @Override
            protected void consume(long offset, ByteBuffer payload) throws Exception {
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                getStreamQueue().put(new Stream(offset, bytes));
            }
        };
        final IIDesc desc = ii.getDescriptor();
        Executors.newSingleThreadExecutor().submit(consumer);
        final Future<?> future = Executors.newSingleThreadExecutor().submit(new IIStreamBuilder(consumer.getStreamQueue(), ii.getSegments().get(0).getStorageLocationIdentifier(), desc, partitionId));
        future.get();
    }
}

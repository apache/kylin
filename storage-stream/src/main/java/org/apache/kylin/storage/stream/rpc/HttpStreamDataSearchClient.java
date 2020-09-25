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

package org.apache.kylin.storage.stream.rpc;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.DataFormatException;

import org.apache.commons.codec.binary.Base64;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.StringCodeSystem;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITuple;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.stream.coordinator.assign.AssignmentsCache;
import org.apache.kylin.stream.core.model.ReplicaSet;
import org.apache.kylin.stream.core.model.DataRequest;
import org.apache.kylin.stream.core.model.DataResponse;
import org.apache.kylin.stream.core.model.Node;
import org.apache.kylin.stream.core.query.ResponseResultSchema;
import org.apache.kylin.stream.core.query.StreamingTupleConverter;
import org.apache.kylin.stream.core.query.StreamingTupleIterator;
import org.apache.kylin.stream.core.storage.Record;
import org.apache.kylin.stream.core.util.NamedThreadFactory;
import org.apache.kylin.stream.core.util.RecordsSerializer;
import org.apache.kylin.stream.core.util.RestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * TODO use long connection rather than short connection
 */
public class HttpStreamDataSearchClient implements IStreamDataSearchClient {
    public static final Logger logger = LoggerFactory.getLogger(HttpStreamDataSearchClient.class);

    private static ExecutorService executorService;
    static {
        executorService = new ThreadPoolExecutor(20, 100, 60L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
                new NamedThreadFactory("stream-rpc-pool-t"));
    }
    private AssignmentsCache assignmentsCache;
    private RestService restService;
    private Map<Node, Long> failedReceivers = Maps.newConcurrentMap();

    public HttpStreamDataSearchClient() {
        assignmentsCache = AssignmentsCache.getInstance();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        int connectionTimeout = kylinConfig.getStreamingRPCHttpConnTimeout();
        int readTimeout = kylinConfig.getStreamingRPCHttpReadTimeout();
        restService = new RestService(connectionTimeout, readTimeout);
    }

    @Override
    public ITupleIterator search(final long minSegmentTime, final CubeInstance cube, final TupleInfo tupleInfo,
            final TupleFilter tupleFilter, final Set<TblColRef> dimensions, final Set<TblColRef> groups,
            final Set<FunctionDesc> metrics, final int storagePushDownLimit, final boolean allowStorageAggregation) {
        List<ReplicaSet> replicaSetsOfCube = assignmentsCache.getReplicaSetsByCube(cube.getName());
        int timeout = 120 * 1000; // timeout should be configurable
        final QueuedStreamingTupleIterator result = new QueuedStreamingTupleIterator(replicaSetsOfCube.size(), timeout);
        final QueryContext query = QueryContextFacade.current();

        final CubeDesc cubeDesc = cube.getDescriptor();
        final ResponseResultSchema schema = new ResponseResultSchema(cubeDesc, dimensions, metrics);
        final StreamingTupleConverter tupleConverter = new StreamingTupleConverter(schema, tupleInfo);
        final RecordsSerializer recordsSerializer = new RecordsSerializer(schema);
        final DataRequest dataRequest = createDataRequest(query.getQueryId(), cube.getName(), minSegmentTime, tupleInfo,
                tupleFilter, dimensions, groups, metrics, storagePushDownLimit, allowStorageAggregation);

        logger.info("Query-{}:send request to stream receivers", query.getQueryId());
        for (final ReplicaSet rs : replicaSetsOfCube) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Iterator<ITuple> tuplesBlock = search(dataRequest, cube, tupleConverter, recordsSerializer, rs,
                                tupleInfo);
                        result.addBlock(tuplesBlock);
                    } catch (Exception e) {
                        result.setEndpointException(e);
                    }
                }
            });
        }

        return result;
    }

    public Iterator<ITuple> search(DataRequest dataRequest, CubeInstance cube, StreamingTupleConverter tupleConverter,
            RecordsSerializer recordsSerializer, ReplicaSet rs, TupleInfo tupleInfo) throws Exception {
        List<Node> receivers = Lists.newArrayList(rs.getNodes());
        Node queryReceiver = findBestReceiverServeQuery(receivers, cube.getName());
        IOException exception;
        try {
            return doSearch(dataRequest, cube, tupleConverter, recordsSerializer, queryReceiver, tupleInfo);
        } catch (IOException e) {
            exception = e;
            failedReceivers.put(queryReceiver, System.currentTimeMillis());
            logger.error("exception throws for receiver:" + queryReceiver + " retry another receiver");
        }

        for (int i = 0; i < receivers.size(); i++) {
            Node receiver = receivers.get(i);
            if (receiver.equals(queryReceiver)) {
                continue;
            }
            try {
                return doSearch(dataRequest, cube, tupleConverter, recordsSerializer, receiver, tupleInfo);
            } catch (IOException e) {
                exception = e;
                failedReceivers.put(receiver, System.currentTimeMillis());
                logger.error("exception throws for receiver:" + receiver + " retry another receiver");
            }

        }
        throw exception;
    }

    private Node findBestReceiverServeQuery(List<Node> receivers, String cubeName) {
        // stick to one receiver according to cube name
        int receiversSize = receivers.size();
        int receiverNo = Math.abs(cubeName.hashCode()) % receiversSize;
        Node foundReceiver = receivers.get(receiverNo);
        Long lastFailTime = failedReceivers.get(foundReceiver);
        if (lastFailTime == null) {
            return foundReceiver;
        }

        if (System.currentTimeMillis() - lastFailTime > 2 * 60 * 1000) { // retry every 2 minutes
            return foundReceiver;
        }

        return receivers.get((receiverNo + 1) % receiversSize);
    }

    public Iterator<ITuple> doSearch(DataRequest dataRequest, CubeInstance cube, StreamingTupleConverter tupleConverter,
            RecordsSerializer recordsSerializer, Node receiver, TupleInfo tupleInfo) throws Exception {
        String queryId = dataRequest.getQueryId();
        logger.info("send query to receiver " + receiver + " with query id:" + queryId);
        String url = "http://" + receiver.getHost() + ":" + receiver.getPort() + "/kylin/api/data/query";

        try {
            String content = JsonUtil.writeValueAsString(dataRequest);
            Stopwatch sw;
            sw = Stopwatch.createUnstarted();
            sw.start();
            int connTimeout = cube.getConfig().getStreamingRPCHttpConnTimeout();
            int readTimeout = cube.getConfig().getStreamingRPCHttpReadTimeout();
            String msg = restService.postRequest(url, content, connTimeout, readTimeout);

            logger.info("query-{}: receive response from {} take time:{}", queryId, receiver, sw.elapsed(MILLISECONDS));
            if (failedReceivers.containsKey(receiver)) {
                failedReceivers.remove(receiver);
            }
            DataResponse response = JsonUtil.readValue(msg, DataResponse.class);
            logger.info("query-{}: receiver {} profile info:{}", queryId, receiver, response.getProfile());
            return deserializeResponse(tupleConverter, recordsSerializer, cube.getName(), tupleInfo, response);
        } catch (Exception e) {
            logger.error("error when search data from receiver:" + url, e);
            throw e;
        }
    }

    public Iterator<ITuple> deserializeResponse(final StreamingTupleConverter tupleConverter,
            final RecordsSerializer recordsSerializer, String cubeName, TupleInfo tupleInfo, DataResponse response)
            throws IOException, DataFormatException {
        final Iterator<Record> records = recordsSerializer.deserialize(Base64.decodeBase64(response.getData()));
        return new StreamingTupleIterator(records, tupleConverter, tupleInfo);
    }

    private DataRequest createDataRequest(String queryId, String cubeName, long minSegmentTime, TupleInfo tupleInfo,
            TupleFilter tupleFilter, Set<TblColRef> dimensions, Set<TblColRef> groups, Set<FunctionDesc> metrics,
            int storagePushDownLimit, boolean allowStorageAggregation) {
        DataRequest request = new DataRequest();
        request.setCubeName(cubeName);
        request.setQueryId(queryId);
        request.setMinSegmentTime(minSegmentTime);
        request.setTupleFilter(
                Base64.encodeBase64String(TupleFilterSerializer.serialize(tupleFilter, StringCodeSystem.INSTANCE)));
        request.setStoragePushDownLimit(storagePushDownLimit);
        request.setAllowStorageAggregation(allowStorageAggregation);
        request.setRequestSendTime(System.currentTimeMillis());
        request.setEnableDetailProfile(BackdoorToggles.isStreamingProfileEnable());
        request.setStorageBehavior(BackdoorToggles.getCoprocessorBehavior());

        Set<String> dimensionSet = Sets.newHashSet();
        for (TblColRef dimension : dimensions) {
            dimensionSet.add(dimension.getCanonicalName());
        }
        request.setDimensions(dimensionSet);

        Set<String> groupSet = Sets.newHashSet();
        for (TblColRef group : groups) {
            groupSet.add(group.getCanonicalName());
        }
        request.setGroups(groupSet);

        request.setMetrics(Lists.newArrayList(metrics));

        return request;
    }

    public static class QueuedStreamingTupleIterator implements ITupleIterator {
        private BlockingQueue<Iterator<ITuple>> queue;

        private Iterator<ITuple> currentBlock = Collections.emptyIterator();

        private int totalBlockNum;
        private int numConsumeBlocks = 0;

        private int timeout;
        private long timeoutTS;
        private volatile Exception endpointException;

        public QueuedStreamingTupleIterator(int blockNum, int timeout) {
            this.queue = new LinkedBlockingQueue<>(blockNum);
            this.totalBlockNum = blockNum;
            this.timeout *= 1.1;
            this.timeoutTS = System.currentTimeMillis() + timeout;
        }

        public void addBlock(Iterator<ITuple> tuples) {
            try {
                queue.put(tuples);
            } catch (InterruptedException e) {
                logger.error("interrupted", e);
                throw new RuntimeException("interrupted", e);
            }
        }

        public void setEndpointException(Exception e) {
            this.endpointException = e;
        }

        private boolean hasEndpointFail() {
            return endpointException != null;
        }

        @Override
        public void close() {
            // do nothing
        }

        @Override
        public boolean hasNext() {
            try {
                if (currentBlock.hasNext()) {
                    return true;
                } else if (numConsumeBlocks < totalBlockNum) {
                    while (numConsumeBlocks < totalBlockNum) {
                        if (hasEndpointFail()) {
                            throw new RuntimeException("endpoint fail", endpointException);
                        }
                        Iterator<ITuple> ret = null;
                        while (ret == null && endpointException == null && timeoutTS > System.currentTimeMillis()) {
                            ret = queue.poll(1000, MILLISECONDS);
                        }
                        currentBlock = ret;
                        if (currentBlock == null) {
                            throw new RuntimeException("timeout when call stream rpc");
                        }
                        numConsumeBlocks++;
                        if (currentBlock.hasNext()) {
                            return true;
                        }
                    }

                }
            } catch (InterruptedException e) {
                logger.error("interrupted", e);
                throw new RuntimeException("interrupted", e);
            }

            return false;
        }

        @Override
        public ITuple next() {
            return currentBlock.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}

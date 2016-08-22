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
package org.apache.kylin.engine.streaming.cube;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.StreamingBatch;
import org.apache.kylin.common.util.StreamingMessage;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.inmemcubing.InMemCubeBuilder;
import org.apache.kylin.cube.util.CubingUtils;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.streaming.StreamingBatchBuilder;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 */
public class StreamingCubeBuilder implements StreamingBatchBuilder {

    private static final Logger logger = LoggerFactory.getLogger(StreamingCubeBuilder.class);

    private final String cubeName;
    private int processedRowCount = 0;

    public StreamingCubeBuilder(String cubeName) {
        this.cubeName = cubeName;
    }

    @Override
    public void build(StreamingBatch streamingBatch, Map<TblColRef, Dictionary<String>> dictionaryMap, ICuboidWriter cuboidWriter) {
        try {
            CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
            final IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(cubeInstance.getDescriptor());
            
            LinkedBlockingQueue<List<String>> blockingQueue = new LinkedBlockingQueue<List<String>>();
            InMemCubeBuilder inMemCubeBuilder = new InMemCubeBuilder(cubeInstance.getDescriptor(), flatDesc, dictionaryMap);
            final Future<?> future = Executors.newCachedThreadPool().submit(inMemCubeBuilder.buildAsRunnable(blockingQueue, cuboidWriter));
            processedRowCount = streamingBatch.getMessages().size();
            for (StreamingMessage streamingMessage : streamingBatch.getMessages()) {
                blockingQueue.put(streamingMessage.getData());
            }
            blockingQueue.put(Collections.<String> emptyList());
            future.get();
            cuboidWriter.flush();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException("error build cube from StreamingBatch", e.getCause());
        } catch (IOException e) {
            throw new RuntimeException("error build cube from StreamingBatch", e.getCause());
        } finally {
            try {
                cuboidWriter.close();
            } catch (IOException e) {
                throw new RuntimeException("error build cube from StreamingBatch", e.getCause());
            }
        }
    }

    @Override
    public IBuildable createBuildable(StreamingBatch streamingBatch) {
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        try {
            CubeSegment segment = cubeManager.appendSegment(cubeInstance, streamingBatch.getTimeRange().getFirst(), streamingBatch.getTimeRange().getSecond(), 0, 0, false);
            segment.setLastBuildJobID(segment.getUuid()); // give a fake job id
            segment.setInputRecords(streamingBatch.getMessages().size());
            segment.setLastBuildTime(System.currentTimeMillis());
            return segment;
        } catch (IOException e) {
            throw new RuntimeException("failed to create IBuildable", e);
        }
    }

    @Override
    public Map<Long, HyperLogLogPlusCounter> sampling(StreamingBatch streamingBatch) {
        final CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        final IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(cubeInstance.getDescriptor());
        long start = System.currentTimeMillis();

        final Map<Long, HyperLogLogPlusCounter> samplingResult = CubingUtils.sampling(cubeInstance.getDescriptor(), flatDesc, Lists.transform(streamingBatch.getMessages(), new Function<StreamingMessage, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable StreamingMessage input) {
                return input.getData();
            }
        }));
        logger.info(String.format("sampling of %d messages cost %d ms", streamingBatch.getMessages().size(), (System.currentTimeMillis() - start)));
        return samplingResult;
    }

    @Override
    public Map<TblColRef, Dictionary<String>> buildDictionary(StreamingBatch streamingBatch, IBuildable buildable) {
        final CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        final Map<TblColRef, Dictionary<String>> dictionaryMap;
        try {
            dictionaryMap = CubingUtils.buildDictionary(cubeInstance, Lists.transform(streamingBatch.getMessages(), new Function<StreamingMessage, List<String>>() {
                @Nullable
                @Override
                public List<String> apply(@Nullable StreamingMessage input) {
                    return input.getData();
                }
            }));
            Map<TblColRef, Dictionary<String>> realDictMap = CubingUtils.writeDictionary((CubeSegment) buildable, dictionaryMap, streamingBatch.getTimeRange().getFirst(), streamingBatch.getTimeRange().getSecond());
            return realDictMap;
        } catch (IOException e) {
            throw new RuntimeException("failed to build dictionary", e);
        }
    }

    @Override
    public void commit(IBuildable buildable) {
        CubeSegment cubeSegment = (CubeSegment) buildable;
        cubeSegment.setStatus(SegmentStatusEnum.READY);
        cubeSegment.setInputRecords(processedRowCount);
        CubeUpdate cubeBuilder = new CubeUpdate(cubeSegment.getCubeInstance());
        cubeBuilder.setToUpdateSegs(cubeSegment);
        try {
            CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).updateCube(cubeBuilder);
        } catch (IOException e) {
            throw new RuntimeException("failed to update CubeSegment", e);
        }
    }
}

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
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.inmemcubing.ICuboidWriter;
import org.apache.kylin.cube.inmemcubing.InMemCubeBuilder;
import org.apache.kylin.cube.util.CubingUtils;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.streaming.StreamingBatch;
import org.apache.kylin.engine.streaming.StreamingBatchBuilder;
import org.apache.kylin.engine.streaming.StreamingMessage;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class StreamingCubeBuilder implements StreamingBatchBuilder {

    private static final Logger logger = LoggerFactory.getLogger(StreamingCubeBuilder.class);

    private final String cubeName;

    public StreamingCubeBuilder(String cubeName) {
        this.cubeName = cubeName;
    }

    @Override
    public void build(StreamingBatch streamingBatch, Map<TblColRef, Dictionary<?>> dictionaryMap, ICuboidWriter cuboidWriter) {
        try {

            CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
            LinkedBlockingQueue<List<String>> blockingQueue = new LinkedBlockingQueue<List<String>>();
            InMemCubeBuilder inMemCubeBuilder = new InMemCubeBuilder(cubeInstance.getDescriptor(), dictionaryMap);
            final Future<?> future = Executors.newCachedThreadPool().submit(inMemCubeBuilder.buildAsRunnable(blockingQueue, cuboidWriter));
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
        }
    }

    @Override
    public IBuildable createBuildable(StreamingBatch streamingBatch) {
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        try {
            return cubeManager.appendSegments(cubeInstance, streamingBatch.getTimeRange().getFirst(), streamingBatch.getTimeRange().getSecond(), false, false);
        } catch (IOException e) {
            throw new RuntimeException("failed to create IBuildable", e);
        }
    }

    @Override
    public Map<Long, HyperLogLogPlusCounter> sampling(StreamingBatch streamingBatch) {
        final CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        long start = System.currentTimeMillis();

        final Map<Long, HyperLogLogPlusCounter> samplingResult = CubingUtils.sampling(cubeInstance.getDescriptor(), Lists.transform(streamingBatch.getMessages(), new Function<StreamingMessage, List<String>>() {
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
    public Map<TblColRef, Dictionary<?>> buildDictionary(StreamingBatch streamingBatch, IBuildable buildable) {
        final CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        final Map<TblColRef, Dictionary<?>> dictionaryMap;
        try {
            dictionaryMap = CubingUtils.buildDictionary(cubeInstance, Lists.transform(streamingBatch.getMessages(), new Function<StreamingMessage, List<String>>() {
                @Nullable
                @Override
                public List<String> apply(@Nullable StreamingMessage input) {
                    return input.getData();
                }
            }));
            Map<TblColRef, Dictionary<?>> realDictMap = writeDictionary((CubeSegment) buildable, dictionaryMap, streamingBatch.getTimeRange().getFirst(), streamingBatch.getTimeRange().getSecond());
            return realDictMap;
        } catch (IOException e) {
            throw new RuntimeException("failed to build dictionary", e);
        }
    }

    private Map<TblColRef, Dictionary<?>> writeDictionary(CubeSegment cubeSegment, Map<TblColRef, Dictionary<?>> dictionaryMap, long startOffset, long endOffset) {
        Map<TblColRef, Dictionary<?>> realDictMap = Maps.newHashMap();

        for (Map.Entry<TblColRef, Dictionary<?>> entry : dictionaryMap.entrySet()) {
            final TblColRef tblColRef = entry.getKey();
            final Dictionary<?> dictionary = entry.getValue();
            ReadableTable.TableSignature signature = new ReadableTable.TableSignature();
            signature.setLastModifiedTime(System.currentTimeMillis());
            signature.setPath(String.format("streaming_%s_%s", startOffset, endOffset));
            signature.setSize(endOffset - startOffset);
            DictionaryInfo dictInfo = new DictionaryInfo(tblColRef.getTable(), tblColRef.getName(), tblColRef.getColumnDesc().getZeroBasedIndex(), tblColRef.getDatatype(), signature);
            logger.info("writing dictionary for TblColRef:" + tblColRef.toString());
            DictionaryManager dictionaryManager = DictionaryManager.getInstance(KylinConfig.getInstanceFromEnv());
            try {
                DictionaryInfo realDict = dictionaryManager.trySaveNewDict(dictionary, dictInfo);
                cubeSegment.putDictResPath(tblColRef, realDict.getResourcePath());
                realDictMap.put(tblColRef, realDict.getDictionaryObject());
            } catch (IOException e) {
                logger.error("error save dictionary for column:" + tblColRef, e);
                throw new RuntimeException("error save dictionary for column:" + tblColRef, e);
            }
        }

        return realDictMap;
    }

    @Override
    public void commit(IBuildable buildable) {
        CubeSegment cubeSegment = (CubeSegment) buildable;
        cubeSegment.setStatus(SegmentStatusEnum.READY);
        CubeUpdate cubeBuilder = new CubeUpdate(cubeSegment.getCubeInstance());
        cubeBuilder.setToAddSegs(cubeSegment);
        try {
            CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).updateCube(cubeBuilder);
        } catch (IOException e) {
            throw new RuntimeException("failed to update CubeSegment", e);
        }
    }
}

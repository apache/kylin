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
package org.apache.kylin.job.streaming;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.inmemcubing.InMemCubeBuilder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.util.CubingUtils;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.FactDistinctColumnsReducer;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable.TableSignature;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.HBaseCuboidWriter;
import org.apache.kylin.storage.hbase.steps.CubeHTableUtil;
import org.apache.kylin.streaming.MicroStreamBatch;
import org.apache.kylin.streaming.MicroStreamBatchConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
public class CubeStreamConsumer implements MicroStreamBatchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CubeStreamConsumer.class);

    private final CubeManager cubeManager;
    private final String cubeName;
    private final KylinConfig kylinConfig;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private int totalConsumedMessageCount = 0;
    private int totalRawMessageCount = 0;

    public CubeStreamConsumer(String cubeName) {
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
        this.cubeManager = CubeManager.getInstance(kylinConfig);
        this.cubeName = cubeName;
    }

    @Override
    public void consume(MicroStreamBatch microStreamBatch) throws Exception {

        totalConsumedMessageCount += microStreamBatch.size();
        totalRawMessageCount += microStreamBatch.getRawMessageCount();

        final List<List<String>> parsedStreamMessages = microStreamBatch.getStreams();
        long startOffset = microStreamBatch.getOffset().getFirst();
        long endOffset = microStreamBatch.getOffset().getSecond();
        LinkedBlockingQueue<List<String>> blockingQueue = new LinkedBlockingQueue<List<String>>(parsedStreamMessages);
        blockingQueue.put(Collections.<String> emptyList());

        final CubeInstance cubeInstance = cubeManager.reloadCubeLocal(cubeName);
        final CubeDesc cubeDesc = cubeInstance.getDescriptor();
        final CubeSegment cubeSegment = cubeManager.appendSegments(cubeManager.getCube(cubeName), microStreamBatch.getTimestamp().getFirst(), microStreamBatch.getTimestamp().getSecond(), false, false);
        long start = System.currentTimeMillis();
        final Map<Long, HyperLogLogPlusCounter> samplingResult = CubingUtils.sampling(cubeInstance.getDescriptor(), parsedStreamMessages);
        logger.info(String.format("sampling of %d messages cost %d ms", parsedStreamMessages.size(), (System.currentTimeMillis() - start)));

        final Configuration conf = HadoopUtil.getCurrentConfiguration();
        final Path outputPath = new Path("file://" + BatchConstants.CFG_STATISTICS_LOCAL_DIR + UUID.randomUUID().toString());
        FactDistinctColumnsReducer.writeCuboidStatistics(conf, outputPath, samplingResult, 100);
        FSDataInputStream localStream = FileSystem.getLocal(conf).open(new Path(outputPath, BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION));
        ResourceStore.getStore(kylinConfig).putResource(cubeSegment.getStatisticsResourcePath(), localStream, System.currentTimeMillis());
        localStream.close();
        FileSystem.getLocal(conf).delete(outputPath, true);

        final Map<TblColRef, Dictionary<?>> dictionaryMap = CubingUtils.buildDictionary(cubeInstance, parsedStreamMessages);
        Map<TblColRef, Dictionary<?>> realDictMap = CubingUtils.writeDictionary(cubeSegment, dictionaryMap, startOffset, endOffset);

        InMemCubeBuilder inMemCubeBuilder = new InMemCubeBuilder(cubeInstance.getDescriptor(), realDictMap);
        final HTableInterface hTable = createHTable(cubeSegment);
        final HBaseCuboidWriter gtRecordWriter = new HBaseCuboidWriter(cubeDesc, hTable);

        executorService.submit(inMemCubeBuilder.buildAsRunnable(blockingQueue, gtRecordWriter)).get();
        gtRecordWriter.flush();
        hTable.close();
        commitSegment(cubeSegment);

        logger.info("Consumed {} messages out of {} raw messages", totalConsumedMessageCount, totalRawMessageCount);
    }

    //TODO: should we use cubeManager.promoteNewlyBuiltSegments?
    private void commitSegment(CubeSegment cubeSegment) throws IOException {
        cubeSegment.setStatus(SegmentStatusEnum.READY);
        CubeUpdate cubeBuilder = new CubeUpdate(cubeSegment.getCubeInstance());
        cubeBuilder.setToAddSegs(cubeSegment);
        CubeManager.getInstance(kylinConfig).updateCube(cubeBuilder);
    }

    private HTableInterface createHTable(final CubeSegment cubeSegment) throws Exception {
        final String hTableName = cubeSegment.getStorageLocationIdentifier();
        CubeHTableUtil.createHTable(cubeSegment.getCubeDesc(), hTableName, null);
        final HTableInterface hTable = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl()).getTable(hTableName);
        logger.info("hTable:" + hTableName + " for segment:" + cubeSegment.getName() + " created!");
        return hTable;
    }

    @Override
    public void stop() {

    }

}

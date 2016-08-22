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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.MemoryBudgetController;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.inmemcubing.DoggedCubeBuilder;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
public class InMemCuboidMapper<KEYIN> extends KylinMapper<KEYIN, Object, ByteArrayWritable, ByteArrayWritable> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidMapper.class);

    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;
    private IMRTableInputFormat flatTableInputFormat;

    private int counter;
    private BlockingQueue<List<String>> queue = new ArrayBlockingQueue<List<String>>(64);
    private Future<?> future;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        cubeSegment = cube.getSegmentById(segmentID);
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSegment).getFlatTableInputFormat();
        IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(cubeSegment);

        Map<TblColRef, Dictionary<String>> dictionaryMap = Maps.newHashMap();

        // dictionary
        for (TblColRef col : cubeDesc.getAllColumnsHaveDictionary()) {
            Dictionary<?> dict = cubeSegment.getDictionary(col);
            if (dict == null) {
                logger.warn("Dictionary for " + col + " was not found.");
            }

            dictionaryMap.put(col, cubeSegment.getDictionary(col));
        }

        DoggedCubeBuilder cubeBuilder = new DoggedCubeBuilder(cube.getDescriptor(), flatDesc, dictionaryMap);
        cubeBuilder.setReserveMemoryMB(calculateReserveMB(context.getConfiguration()));

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        future = executorService.submit(cubeBuilder.buildAsRunnable(queue, new MapContextGTRecordWriter(context, cubeDesc, cubeSegment)));

    }

    private int calculateReserveMB(Configuration configuration) {
        int sysAvailMB = MemoryBudgetController.getSystemAvailMB();
        int mrReserve = configuration.getInt("mapreduce.task.io.sort.mb", 100);
        int sysReserve = Math.max(sysAvailMB / 10, 100);
        int reserveMB = mrReserve + sysReserve;
        logger.info("Reserve " + reserveMB + " MB = " + mrReserve + " (MR reserve) + " + sysReserve + " (SYS reserve)");
        return reserveMB;
    }

    @Override
    public void map(KEYIN key, Object record, Context context) throws IOException, InterruptedException {
        // put each row to the queue
        String[] row = flatTableInputFormat.parseMapperInput(record);
        List<String> rowAsList = Arrays.asList(row);

        while (!future.isDone()) {
            if (queue.offer(rowAsList, 1, TimeUnit.SECONDS)) {
                counter++;
                if (counter % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                    logger.info("Handled " + counter + " records!");
                }
                break;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("Totally handled " + counter + " records!");

        while (!future.isDone()) {
            if (queue.offer(Collections.<String> emptyList(), 1, TimeUnit.SECONDS)) {
                break;
            }
        }

        try {
            future.get();
        } catch (Exception e) {
            throw new IOException("Failed to build cube in mapper " + context.getTaskAttemptID().getTaskID().getId(), e);
        }
        queue.clear();
    }

}

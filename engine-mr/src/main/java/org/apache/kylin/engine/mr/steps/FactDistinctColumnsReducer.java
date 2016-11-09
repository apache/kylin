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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class FactDistinctColumnsReducer extends KylinReducer<Text, Text, NullWritable, Text> {

    private List<TblColRef> columnList;
    private String statisticsOutput = null;
    private List<Long> baseCuboidRowCountInMappers;
    protected Map<Long, HyperLogLogPlusCounter> cuboidHLLMap = null;
    protected long baseCuboidId;
    protected CubeDesc cubeDesc;
    private long totalRowsBeforeMerge = 0;
    private int samplingPercentage;
    private List<ByteArray> colValues;
    private TblColRef col = null;
    private boolean isStatistics = false;
    private boolean isPartitionCol = false;
    private KylinConfig cubeConfig;
    private int uhcReducerCount;
    private Map<Integer, Integer> ReducerIdToColumnIndex = new HashMap<>();
    private int taskId;

    protected static final Logger logger = LoggerFactory.getLogger(FactDistinctColumnsReducer.class);

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeConfig = cube.getConfig();
        cubeDesc = cube.getDescriptor();
        columnList = CubeManager.getInstance(config).getAllDictColumnsOnFact(cubeDesc);

        boolean collectStatistics = Boolean.parseBoolean(conf.get(BatchConstants.CFG_STATISTICS_ENABLED));
        int numberOfTasks = context.getNumReduceTasks();
        taskId = context.getTaskAttemptID().getTaskID().getId();

        uhcReducerCount = cube.getConfig().getUHCReducerCount();
        initReducerIdToColumnIndex(config);

        if (collectStatistics && (taskId == numberOfTasks - 1)) {
            // hll
            isStatistics = true;
            statisticsOutput = conf.get(BatchConstants.CFG_STATISTICS_OUTPUT);
            baseCuboidRowCountInMappers = Lists.newArrayList();
            cuboidHLLMap = Maps.newHashMap();
            samplingPercentage = Integer.parseInt(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT));
        } else if (collectStatistics && (taskId == numberOfTasks - 2)) {
            // partition col
            isStatistics = false;
            isPartitionCol = true;
            col = cubeDesc.getModel().getPartitionDesc().getPartitionDateColumnRef();
            colValues = Lists.newLinkedList();
        } else {
            // col
            isStatistics = false;
            isPartitionCol = false;
            col = columnList.get(ReducerIdToColumnIndex.get(taskId));
            colValues = Lists.newLinkedList();
        }
    }

    private void initReducerIdToColumnIndex(KylinConfig config) throws IOException {
        int[] uhcIndex = CubeManager.getInstance(config).getUHCIndex(cubeDesc);
        int count = 0;
        for (int i = 0; i < uhcIndex.length; i++) {
            ReducerIdToColumnIndex.put(count * (uhcReducerCount - 1) + i, i);
            if (uhcIndex[i] == 1) {
                for (int j = 1; j < uhcReducerCount; j++) {
                    ReducerIdToColumnIndex.put(count * (uhcReducerCount - 1) + j + i, i);
                }
                count++;
            }
        }
    }

    @Override
    public void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        if (isStatistics == true) {
            // for hll
            long cuboidId = Bytes.toLong(key.getBytes(), 1, Bytes.SIZEOF_LONG);
            for (Text value : values) {
                HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(cubeConfig.getCubeStatsHLLPrecision());
                ByteBuffer bf = ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
                hll.readRegisters(bf);

                totalRowsBeforeMerge += hll.getCountEstimate();

                if (cuboidId == baseCuboidId) {
                    baseCuboidRowCountInMappers.add(hll.getCountEstimate());
                }

                if (cuboidHLLMap.get(cuboidId) != null) {
                    cuboidHLLMap.get(cuboidId).merge(hll);
                } else {
                    cuboidHLLMap.put(cuboidId, hll);
                }
            }
        } else {
            colValues.add(new ByteArray(Bytes.copy(key.getBytes(), 1, key.getLength() - 1)));
            if (colValues.size() == 1000000) { //spill every 1 million
                logger.info("spill values to disk...");
                outputDistinctValues(col, colValues, context);
                colValues.clear();
            }
        }

    }

    private void outputDistinctValues(TblColRef col, Collection<ByteArray> values, Context context) throws IOException {
        final Configuration conf = context.getConfiguration();
        final FileSystem fs = FileSystem.get(conf);
        final String outputPath = conf.get(BatchConstants.CFG_OUTPUT_PATH);
        final Path colDir = new Path(outputPath, col.getName());
        final String fileName = col.getName() + "-" + taskId % uhcReducerCount;
        final Path outputFile = new Path(colDir, fileName);

        FSDataOutputStream out = null;
        try {
            if (!fs.exists(colDir)) {
                fs.mkdirs(colDir);
            }

            if (fs.exists(outputFile)) {
                out = fs.append(outputFile);
                logger.info("append file " + outputFile);
            } else {
                out = fs.create(outputFile);
                logger.info("create file " + outputFile);
            }

            for (ByteArray value : values) {
                out.write(value.array(), value.offset(), value.length());
                out.write('\n');
            }
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        if (isStatistics == false) {
            if (colValues.size() > 0) {
                outputDistinctValues(col, colValues, context);
                colValues.clear();
            }
        } else {
            //output the hll info;
            long grandTotal = 0;
            for (HyperLogLogPlusCounter hll : cuboidHLLMap.values()) {
                grandTotal += hll.getCountEstimate();
            }
            double mapperOverlapRatio = grandTotal == 0 ? 0 : (double) totalRowsBeforeMerge / grandTotal;

            int mapperNumber = baseCuboidRowCountInMappers.size();

            writeMapperAndCuboidStatistics(context); // for human check
            CubeStatsWriter.writeCuboidStatistics(context.getConfiguration(), new Path(statisticsOutput), //
                    cuboidHLLMap, samplingPercentage, mapperNumber, mapperOverlapRatio);
        }
    }

    private void writeMapperAndCuboidStatistics(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path(statisticsOutput, BatchConstants.CFG_STATISTICS_CUBE_ESTIMATION_FILENAME));

        try {
            String msg;

            List<Long> allCuboids = Lists.newArrayList();
            allCuboids.addAll(cuboidHLLMap.keySet());
            Collections.sort(allCuboids);

            msg = "Total cuboid number: \t" + allCuboids.size();
            writeLine(out, msg);
            msg = "Samping percentage: \t" + samplingPercentage;
            writeLine(out, msg);

            writeLine(out, "The following statistics are collected based on sampling data.");
            writeLine(out, "Number of Mappers: " + baseCuboidRowCountInMappers.size());
            for (int i = 0; i < baseCuboidRowCountInMappers.size(); i++) {
                if (baseCuboidRowCountInMappers.get(i) > 0) {
                    msg = "Base Cuboid in Mapper " + i + " row count: \t " + baseCuboidRowCountInMappers.get(i);
                    writeLine(out, msg);
                }
            }

            long grantTotal = 0;
            for (long i : allCuboids) {
                grantTotal += cuboidHLLMap.get(i).getCountEstimate();
                msg = "Cuboid " + i + " row count is: \t " + cuboidHLLMap.get(i).getCountEstimate();
                writeLine(out, msg);
            }

            msg = "Sum of all the cube segments (before merge) is: \t " + totalRowsBeforeMerge;
            writeLine(out, msg);

            msg = "After merge, the cube has row count: \t " + grantTotal;
            writeLine(out, msg);

            if (grantTotal > 0) {
                msg = "The mapper overlap ratio is: \t" + totalRowsBeforeMerge / grantTotal;
                writeLine(out, msg);
            }

        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    private void writeLine(FSDataOutputStream out, String msg) throws IOException {
        out.write(msg.getBytes());
        out.write('\n');

    }

}

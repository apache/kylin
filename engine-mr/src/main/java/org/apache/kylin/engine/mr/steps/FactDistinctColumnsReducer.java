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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.IDictionaryBuilder;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class FactDistinctColumnsReducer extends KylinReducer<SelfDefineSortableKey, Text, NullWritable, Text> {

    private static final Logger logger = LoggerFactory.getLogger(FactDistinctColumnsReducer.class);

    private List<Long> baseCuboidRowCountInMappers;
    protected Map<Long, HLLCounter> cuboidHLLMap = null;
    protected long baseCuboidId;
    protected CubeDesc cubeDesc;
    private long totalRowsBeforeMerge = 0;
    private int samplingPercentage;
    private TblColRef col = null;
    private boolean isStatistics = false;
    private KylinConfig cubeConfig;
    private int taskId;
    private int rowCount = 0;
    private FactDistinctColumnsReducerMapping reducerMapping;

    //local build dict
    private boolean buildDictInReducer;
    private IDictionaryBuilder builder;
    private String maxValue = null;
    private String minValue = null;
    public static final String DICT_FILE_POSTFIX = ".rldict";
    public static final String DIMENSION_COL_INFO_FILE_POSTFIX = ".dci";

    private MultipleOutputs mos;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        Configuration conf = context.getConfiguration();
        mos = new MultipleOutputs(context);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeConfig = cube.getConfig();
        cubeDesc = cube.getDescriptor();

        taskId = context.getTaskAttemptID().getTaskID().getId();

        reducerMapping = new FactDistinctColumnsReducerMapping(cube);

        logger.info("reducer no " + taskId + ", role play " + reducerMapping.getRolePlayOfReducer(taskId));

        if (reducerMapping.isCuboidRowCounterReducer(taskId)) {
            // hll
            isStatistics = true;
            baseCuboidId = cube.getCuboidScheduler().getBaseCuboidId();
            baseCuboidRowCountInMappers = Lists.newArrayList();
            cuboidHLLMap = Maps.newHashMap();
            samplingPercentage = Integer
                    .parseInt(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT));
            logger.info("Reducer " + taskId + " handling stats");
        } else {
            // normal col
            col = reducerMapping.getColForReducer(taskId);
            Preconditions.checkNotNull(col);

            // local build dict
            buildDictInReducer = config.isBuildDictInReducerEnabled();
            if (cubeDesc.getDictionaryBuilderClass(col) != null) { // only works with default dictionary builder
                buildDictInReducer = false;
            }
            if (reducerMapping.getReducerNumForDimCol(col) > 1) {
                buildDictInReducer = false; // only works if this is the only reducer of a dictionary column
            }
            if (buildDictInReducer) {
                builder = DictionaryGenerator.newDictionaryBuilder(col.getType());
                builder.init(null, 0, null);
            }
            logger.info("Reducer " + taskId + " handling column " + col + ", buildDictInReducer=" + buildDictInReducer);
        }
    }

    @Override
    public void doReduce(SelfDefineSortableKey skey, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Text key = skey.getText();
        if (isStatistics) {
            // for hll
            long cuboidId = Bytes.toLong(key.getBytes(), 1, Bytes.SIZEOF_LONG);
            for (Text value : values) {
                HLLCounter hll = new HLLCounter(cubeConfig.getCubeStatsHLLPrecision());
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
            String value = Bytes.toString(key.getBytes(), 1, key.getLength() - 1);
            logAFewRows(value);
            // if dimension col, compute max/min value
            if (cubeDesc.listDimensionColumnsExcludingDerived(true).contains(col) && col.getType().needCompare()) {
                if (minValue == null || col.getType().compare(minValue, value) > 0) {
                    minValue = value;
                }
                if (maxValue == null || col.getType().compare(maxValue, value) < 0) {
                    maxValue = value;
                }
            }

            //if dict column
            if (cubeDesc.getAllColumnsNeedDictionaryBuilt().contains(col)) {
                if (buildDictInReducer) {
                    builder.addValue(value);
                } else {
                    byte[] keyBytes = Bytes.copy(key.getBytes(), 1, key.getLength() - 1);
                    // output written to baseDir/colName/-r-00000 (etc)
                    String fileName = col.getIdentity() + "/";
                    mos.write(BatchConstants.CFG_OUTPUT_COLUMN, NullWritable.get(), new Text(keyBytes), fileName);
                }
            }
        }

        rowCount++;
    }

    private void logAFewRows(String value) {
        if (rowCount < 10) {
            logger.info("Received value: " + value);
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        if (isStatistics) {
            //output the hll info;
            List<Long> allCuboids = Lists.newArrayList();
            allCuboids.addAll(cuboidHLLMap.keySet());
            Collections.sort(allCuboids);

            logMapperAndCuboidStatistics(allCuboids); // for human check
            outputStatistics(allCuboids);
        } else {
            //dimension col
            if (cubeDesc.listDimensionColumnsExcludingDerived(true).contains(col)) {
                outputDimRangeInfo();
            }
            // dic col
            if (buildDictInReducer) {
                Dictionary<String> dict = builder.build();
                outputDict(col, dict);
            }
        }

        mos.close();
    }

    private void outputDimRangeInfo() throws IOException, InterruptedException {
        if (col != null && minValue != null) {
            // output written to baseDir/colName/colName.dci-r-00000 (etc)
            String dimRangeFileName = col.getIdentity() + "/" + col.getName() + DIMENSION_COL_INFO_FILE_POSTFIX;

            mos.write(BatchConstants.CFG_OUTPUT_PARTITION, NullWritable.get(),
                    new Text(minValue.getBytes(StandardCharsets.UTF_8)), dimRangeFileName);
            mos.write(BatchConstants.CFG_OUTPUT_PARTITION, NullWritable.get(),
                    new Text(maxValue.getBytes(StandardCharsets.UTF_8)), dimRangeFileName);
            logger.info("write dimension range info for col : " + col.getName() + "  minValue:" + minValue
                    + " maxValue:" + maxValue);
        }
    }

    private void outputDict(TblColRef col, Dictionary<String> dict) throws IOException, InterruptedException {
        // output written to baseDir/colName/colName.rldict-r-00000 (etc)
        String dictFileName = col.getIdentity() + "/" + col.getName() + DICT_FILE_POSTFIX;

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream outputStream = new DataOutputStream(baos);) {
            outputStream.writeUTF(dict.getClass().getName());
            dict.write(outputStream);

            mos.write(BatchConstants.CFG_OUTPUT_DICT, NullWritable.get(),
                    new ArrayPrimitiveWritable(baos.toByteArray()), dictFileName);
        }
    }

    private void outputStatistics(List<Long> allCuboids) throws IOException, InterruptedException {
        // output written to baseDir/statistics/statistics-r-00000 (etc)
        String statisticsFileName = BatchConstants.CFG_OUTPUT_STATISTICS + "/" + BatchConstants.CFG_OUTPUT_STATISTICS;

        ByteBuffer valueBuf = ByteBuffer.allocate(BufferedMeasureCodec.DEFAULT_BUFFER_SIZE);

        // mapper overlap ratio at key -1
        long grandTotal = 0;
        for (HLLCounter hll : cuboidHLLMap.values()) {
            grandTotal += hll.getCountEstimate();
        }
        double mapperOverlapRatio = grandTotal == 0 ? 0 : (double) totalRowsBeforeMerge / grandTotal;
        mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(-1),
                new BytesWritable(Bytes.toBytes(mapperOverlapRatio)), statisticsFileName);

        // mapper number at key -2
        mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(-2),
                new BytesWritable(Bytes.toBytes(baseCuboidRowCountInMappers.size())), statisticsFileName);

        // sampling percentage at key 0
        mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(0L),
                new BytesWritable(Bytes.toBytes(samplingPercentage)), statisticsFileName);

        for (long i : allCuboids) {
            valueBuf.clear();
            cuboidHLLMap.get(i).writeRegisters(valueBuf);
            valueBuf.flip();
            mos.write(BatchConstants.CFG_OUTPUT_STATISTICS, new LongWritable(i),
                    new BytesWritable(valueBuf.array(), valueBuf.limit()), statisticsFileName);
        }
    }

    private void logMapperAndCuboidStatistics(List<Long> allCuboids) throws IOException {
        logger.info("Cuboid number for task: " + taskId + "\t" + allCuboids.size());
        logger.info("Samping percentage: \t" + samplingPercentage);
        logger.info("The following statistics are collected based on sampling data.");
        logger.info("Number of Mappers: " + baseCuboidRowCountInMappers.size());

        for (int i = 0; i < baseCuboidRowCountInMappers.size(); i++) {
            if (baseCuboidRowCountInMappers.get(i) > 0) {
                logger.info("Base Cuboid in Mapper " + i + " row count: \t " + baseCuboidRowCountInMappers.get(i));
            }
        }

        long grantTotal = 0;
        for (long i : allCuboids) {
            grantTotal += cuboidHLLMap.get(i).getCountEstimate();
            logger.info("Cuboid " + i + " row count is: \t " + cuboidHLLMap.get(i).getCountEstimate());
        }

        logger.info("Sum of row counts (before merge) is: \t " + totalRowsBeforeMerge);
        logger.info("After merge, the row count: \t " + grantTotal);
    }

}

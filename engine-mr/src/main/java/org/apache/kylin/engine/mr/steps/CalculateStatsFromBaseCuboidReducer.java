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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class CalculateStatsFromBaseCuboidReducer extends KylinReducer<Text, Text, NullWritable, Text> {

    private static final Logger logger = LoggerFactory.getLogger(CalculateStatsFromBaseCuboidReducer.class);

    private KylinConfig cubeConfig;
    protected long baseCuboidId;
    protected Map<Long, HLLCounter> cuboidHLLMap = null;
    private List<Long> baseCuboidRowCountInMappers;
    private long totalRowsBeforeMerge = 0;

    private String output = null;
    private int samplingPercentage;

    private int taskId;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeConfig = cube.getConfig();

        baseCuboidId = cube.getCuboidScheduler().getBaseCuboidId();
        baseCuboidRowCountInMappers = Lists.newLinkedList();

        output = conf.get(BatchConstants.CFG_OUTPUT_PATH);
        samplingPercentage = Integer
                .parseInt(context.getConfiguration().get(BatchConstants.CFG_STATISTICS_SAMPLING_PERCENT));

        taskId = context.getTaskAttemptID().getTaskID().getId();
        cuboidHLLMap = Maps.newHashMap();
    }

    @Override
    public void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long cuboidId = Bytes.toLong(key.getBytes());
        logger.info("Cuboid id to be processed: " + cuboidId);
        for (Text value : values) {
            HLLCounter hll = new HLLCounter(cubeConfig.getCubeStatsHLLPrecision());
            ByteBuffer bf = ByteBuffer.wrap(value.getBytes(), 0, value.getLength());
            hll.readRegisters(bf);

            if (cuboidId == baseCuboidId) {
                baseCuboidRowCountInMappers.add(hll.getCountEstimate());
            }

            totalRowsBeforeMerge += hll.getCountEstimate();

            if (cuboidHLLMap.get(cuboidId) != null) {
                cuboidHLLMap.get(cuboidId).merge(hll);
            } else {
                cuboidHLLMap.put(cuboidId, hll);
            }
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        long grandTotal = 0;
        for (HLLCounter hll : cuboidHLLMap.values()) {
            grandTotal += hll.getCountEstimate();
        }
        double mapperOverlapRatio = grandTotal == 0 ? 0 : (double) totalRowsBeforeMerge / grandTotal;

        CubeStatsWriter.writePartialCuboidStatistics(context.getConfiguration(), new Path(output), //
                cuboidHLLMap, samplingPercentage, baseCuboidRowCountInMappers.size(), mapperOverlapRatio, taskId);
    }
}

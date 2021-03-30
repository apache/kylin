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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import java.util.Locale;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author George Song (ysong1)
 * 
 */
public class CuboidReducer extends KylinReducer<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(CuboidReducer.class);

    private String cubeName;
    private CubeDesc cubeDesc;
    private List<MeasureDesc> measuresDescs;

    private BufferedMeasureCodec codec;
    private MeasureAggregators aggs;

    private int cuboidLevel;
    private int[] needAggrMeasures;
    private Object[] input;
    private Object[] result;
    private int vcounter;

    private Text outputValue = new Text();

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase(Locale.ROOT);

        // only used in Build job, not in Merge job
        cuboidLevel = context.getConfiguration().getInt(BatchConstants.CFG_CUBE_CUBOID_LEVEL, 0);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeDesc = CubeManager.getInstance(config).getCube(cubeName).getDescriptor();
        measuresDescs = cubeDesc.getMeasures();

        codec = new BufferedMeasureCodec(measuresDescs);
        aggs = new MeasureAggregators(measuresDescs);

        input = new Object[measuresDescs.size()];
        result = new Object[measuresDescs.size()];

        List<Integer> needAggMeasuresList = Lists.newArrayList();
        for (int i = 0; i < measuresDescs.size(); i++) {
            if (cuboidLevel == 0) {
                needAggMeasuresList.add(i);
            } else {
                if (!measuresDescs.get(i).getFunction().getMeasureType().onlyAggrInBaseCuboid()) {
                    needAggMeasuresList.add(i);
                }
            }
        }

        needAggrMeasures = new int[needAggMeasuresList.size()];
        for (int i = 0; i < needAggMeasuresList.size(); i++) {
            needAggrMeasures[i] = needAggMeasuresList.get(i);
        }
    }

    @Override
    public void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        aggs.reset();

        for (Text value : values) {
            if (vcounter++ % BatchConstants.NORMAL_RECORD_LOG_THRESHOLD == 0) {
                logger.info("Handling value with ordinal (This is not KV number!): " + vcounter);
            }
            codec.decode(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()), input);
            aggs.aggregate(input, needAggrMeasures);
        }
        aggs.collectStates(result);

        ByteBuffer valueBuf = codec.encode(result);

        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(key, outputValue);
    }
}

/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.hadoop.cube;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.kv.RowConstants;
import com.kylinolap.cube.measure.MeasureAggregators;
import com.kylinolap.cube.measure.MeasureCodec;
import com.kylinolap.cube.model.CubeDesc;
import com.kylinolap.cube.model.MeasureDesc;
import com.kylinolap.job.constant.BatchConstants;
import com.kylinolap.job.hadoop.AbstractHadoopJob;

/**
 * @author George Song (ysong1)
 * 
 */
public class CuboidReducer extends Reducer<Text, Text, Text, Text> {

    private static final Logger logger = LoggerFactory.getLogger(CuboidReducer.class);

    private String cubeName;
    private CubeDesc cubeDesc;
    private List<MeasureDesc> measuresDescs;

    private MeasureCodec codec;
    private MeasureAggregators aggs;

    private int counter;
    private Object[] input;
    private Object[] result;

    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException {
        cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(context.getConfiguration());

        cubeDesc = CubeManager.getInstance(config).getCube(cubeName).getDescriptor();
        measuresDescs = cubeDesc.getMeasures();

        codec = new MeasureCodec(measuresDescs);
        aggs = new MeasureAggregators(measuresDescs);

        input = new Object[measuresDescs.size()];
        result = new Object[measuresDescs.size()];
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        aggs.reset();

        for (Text value : values) {
            codec.decode(value, input);
            aggs.aggregate(input);
        }
        aggs.collectStates(result);

        valueBuf.clear();
        codec.encode(result, valueBuf);

        outputValue.set(valueBuf.array(), 0, valueBuf.position());
        context.write(key, outputValue);

        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }

}

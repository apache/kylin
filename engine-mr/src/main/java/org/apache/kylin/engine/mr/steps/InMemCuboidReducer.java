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

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.measure.MeasureAggregators;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class InMemCuboidReducer extends KylinReducer<ByteArrayWritable, ByteArrayWritable, Object, Object> {

    private static final Logger logger = LoggerFactory.getLogger(InMemCuboidReducer.class);

    private MeasureCodec codec;
    private MeasureAggregators aggs;

    private int counter;
    private Object[] input;
    private Object[] result;
    
    private Text outputKey;
    private Text outputValue;
    private ByteBuffer valueBuf;

    @Override
    protected void setup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME).toUpperCase();
        String segmentName = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_NAME);
        boolean isMerge = Boolean.parseBoolean(context.getConfiguration().get(BatchConstants.CFG_IS_MERGE));

        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        CubeSegment cubeSeg = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        List<MeasureDesc> measuresDescs = cubeDesc.getMeasures();
        codec = new MeasureCodec(measuresDescs);
        aggs = new MeasureAggregators(measuresDescs);
        input = new Object[measuresDescs.size()];
        result = new Object[measuresDescs.size()];
        
        outputKey = new Text();
        outputValue = new Text();
        valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    }

    @Override
    public void reduce(ByteArrayWritable key, Iterable<ByteArrayWritable> values, Context context) throws IOException, InterruptedException {

        aggs.reset();

        for (ByteArrayWritable value : values) {
            codec.decode(value.asBuffer(), input);
            aggs.aggregate(input);
        }
        aggs.collectStates(result);

        // output key
        outputKey.set(key.array(), key.offset(), key.length());

        // output value
        valueBuf.clear();
        codec.encode(result, valueBuf);
        outputValue.set(valueBuf.array(), 0, valueBuf.position());

        context.write(outputKey, outputValue);
        
        counter++;
        if (counter % BatchConstants.COUNTER_MAX == 0) {
            logger.info("Handled " + counter + " records!");
        }
    }

}

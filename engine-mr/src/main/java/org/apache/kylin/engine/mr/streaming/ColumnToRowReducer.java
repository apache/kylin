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

package org.apache.kylin.engine.mr.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.KylinReducer;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.measure.MeasureAggregators;
import org.apache.kylin.metadata.model.MeasureDesc;

public class ColumnToRowReducer extends KylinReducer<Text, Text, Text, Text> {
    private String cubeName;
    private CubeDesc cubeDesc;
    private CubeInstance cube;
    private List<MeasureDesc> measures;

    @Override
    protected void doSetup(Context context) throws IOException, InterruptedException {
        super.doSetup(context);
        Configuration conf = context.getConfiguration();
        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        if (cubeName == null) {
            throw new IllegalArgumentException("Can not find cube. ");
        }
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();
        cube = CubeManager.getInstance(config).getCube(cubeName.toUpperCase(Locale.ROOT));
        cubeDesc = cube.getDescriptor();
        //        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        measures = cubeDesc.getMeasures();
    }

    @Override
    protected void doReduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //        super.reduce(key, values, context);
        context.write(key, mergeRecord(values));
    }

    private Text mergeRecord(Iterable<Text> values) {
        Object[] aggResult = new Object[measures.size()];
        Iterator<Text> iterator = values.iterator();
        BufferedMeasureCodec codec = new BufferedMeasureCodec(measures);
        MeasureAggregators aggregators = new MeasureAggregators(measures);
        Object[] metrics = new Object[measures.size()];
        while (iterator.hasNext()) {
            byte[] bytes = iterator.next().getBytes();
            codec.decode(ByteBuffer.wrap(bytes), metrics);
            aggregators.aggregate(metrics);
        }
        aggregators.collectStates(aggResult);
        ByteBuffer valueBuffer = codec.encode(aggResult);
        Text result = new Text();
        result.set(valueBuffer.array(), 0, valueBuffer.position());
        return result;
    }
}

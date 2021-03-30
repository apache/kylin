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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.IMROutput2;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;

/**
 * @author ysong1, honma
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MergeCuboidMapper extends KylinMapper<Text, Text, Text, Text> {

    private SegmentReEncoder reEncoder;
    private Pair<Text, Text> newPair;

    @Override
    protected void doSetup(Context context) throws IOException, InterruptedException {
        super.bindCurrentConfiguration(context.getConfiguration());

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cube = cubeManager.getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        CubeSegment mergedCubeSegment = cube.getSegmentById(segmentID);

        // decide which source segment
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        IMROutput2.IMRMergeOutputFormat outputFormat = MRUtil.getBatchMergeOutputSide2(mergedCubeSegment)
                .getOutputFormat();
        CubeSegment sourceCubeSegment = outputFormat.findSourceSegment(fileSplit, cube);
        reEncoder = new SegmentReEncoder(cubeDesc, sourceCubeSegment, mergedCubeSegment, config);
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        newPair = reEncoder.reEncode(key, value);
        context.write(newPair.getFirst(), newPair.getSecond());
    }
}

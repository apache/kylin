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
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

public class FilterRecommendCuboidDataMapper extends KylinMapper<Text, Text, Text, Text> {

    private boolean enableSharding;
    private long baseCuboid;
    private Set<Long> recommendCuboids;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cube = cubeManager.getCube(cubeName);
        CubeSegment optSegment = cube.getSegmentById(segmentID);
        CubeSegment originalSegment = cube.getOriginalSegmentToOptimize(optSegment);

        enableSharding = originalSegment.isEnableSharding();
        baseCuboid = cube.getCuboidScheduler().getBaseCuboidId();

        recommendCuboids = cube.getCuboidsRecommend();
        Preconditions.checkNotNull(recommendCuboids, "The recommend cuboid map could not be null");
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidID = RowKeySplitter.getCuboidId(key.getBytes(), enableSharding);
        if (cuboidID != baseCuboid && !recommendCuboids.contains(cuboidID)) {
            return;
        }

        context.write(key, value);
    }
}

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

package org.apache.kylin.engine.mr.common;

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class CuboidShardUtil {
    protected static final Logger logger = LoggerFactory.getLogger(CuboidShardUtil.class);

    public static void saveCuboidShards(CubeSegment segment, Map<Long, Short> cuboidShards, int totalShards) throws IOException {
        CubeManager cubeManager = CubeManager.getInstance(segment.getConfig());

        Map<Long, Short> filtered = Maps.newHashMap();
        for (Map.Entry<Long, Short> entry : cuboidShards.entrySet()) {
            if (entry.getValue() <= 1) {
                logger.info("Cuboid {} has {} shards, skip saving it as an optimization", entry.getKey(), entry.getValue());
            } else {
                logger.info("Cuboid {} has {} shards, saving it", entry.getKey(), entry.getValue());
                filtered.put(entry.getKey(), entry.getValue());
            }
        }

        segment.setCuboidShardNums(filtered);
        segment.setTotalShards(totalShards);

        CubeUpdate cubeBuilder = new CubeUpdate(segment.getCubeInstance());
        cubeBuilder.setToUpdateSegs(segment);
        cubeManager.updateCube(cubeBuilder);
    }
}

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

package org.apache.kylin.storage.druid.write;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.CuboidShardUtil;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

// calculate and save cuboid->shards map based on cuboid size stats
public class CalculateShardsStep extends AbstractExecutable {
    private final BufferedLogger stepLogger = new BufferedLogger(logger);

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final Map<String, String> params = getParams();
        final String cubeName = CubingExecutableUtil.getCubeName(params);
        final String segmentID = CubingExecutableUtil.getSegmentId(params);
        stepLogger.log("cube: " + cubeName + ", segment: " + segmentID);

        try {
            CubeManager manager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
            CubeInstance cube = manager.getCube(cubeName);
            CubeSegment segment = cube.getSegmentById(segmentID);

            CubeStatsReader statsReader = new CubeStatsReader(segment, segment.getConfig());
            Map<Long, Double> cuboidSizeMap = statsReader.getCuboidSizeMap();

            Pair<Integer, Map<Long, Short>> result = calculateCuboidPartitions(
                    segment.getConfig(), cuboidSizeMap);
            CuboidShardUtil.saveCuboidShards(segment, result.getSecond(), result.getFirst());

            return new ExecuteResult(ExecuteResult.State.SUCCEED, stepLogger.getBufferedLog());

        } catch (Exception e) {
            logger.error("CalculateShardsStep failed", e);
            stepLogger.log("FAILED! " + e.getMessage());
            return new ExecuteResult(ExecuteResult.State.ERROR, stepLogger.getBufferedLog());
        }
    }

    // return (total_shards, cuboid=>num_shards) tuple
    private Pair<Integer, Map<Long, Short>> calculateCuboidPartitions(
            KylinConfig cubeConfig, Map<Long, Double> cuboidSizeMap) {

        double totalSizeMB = 0;
        for (double size : cuboidSizeMap.values()) {
            totalSizeMB += size;
        }

        int totalShards = (int) Math.round(totalSizeMB / (cubeConfig.getDruidShardCutGB() * 1024));
        totalShards = Math.max(totalShards, cubeConfig.getDruidMinShardCount());
        totalShards = Math.min(totalShards, cubeConfig.getDruidMaxShardCount());
        stepLogger.log("Estimated total segment size = " + totalSizeMB + " MB, choose " + totalShards + " shards");

        // decide shard count for each cuboid, based on cuboid size
        Map<Long, Short> cuboidShards = new HashMap<>();
        final double mbPerShard =  totalSizeMB / totalShards;

        Set<Long> cuboids = new TreeSet<>();
        cuboids.addAll(cuboidSizeMap.keySet());

        for (Long cuboid : cuboids) {
            double estimatedSize = cuboidSizeMap.get(cuboid);
            final double magic = 23;
            int nShards = (int) (estimatedSize * magic / mbPerShard + 1);
            nShards = Math.max(nShards, 1);
            nShards = Math.min(nShards, totalShards);

            cuboidShards.put(cuboid, (short) nShards);
        }

        return new Pair<>(totalShards, cuboidShards);
    }

    public void setCubeName(String cubeName) {
        CubingExecutableUtil.setCubeName(cubeName, getParams());
    }

    public void setSegmentID(String segmentID) {
        CubingExecutableUtil.setSegmentId(segmentID, getParams());
    }

}
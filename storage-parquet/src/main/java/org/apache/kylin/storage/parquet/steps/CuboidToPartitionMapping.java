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
package org.apache.kylin.storage.parquet.steps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Yichen on 11/12/18.
 */
public class CuboidToPartitionMapping implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(CuboidToPartitionMapping.class);

    private Map<Long, List<Integer>> cuboidPartitions;
    private int partitionNum;

    public CuboidToPartitionMapping(Map<Long, List<Integer>> cuboidPartitions) {
        this.cuboidPartitions = cuboidPartitions;
        int partitions = 0;
        for (Map.Entry<Long, List<Integer>> entry : cuboidPartitions.entrySet()) {
            partitions = partitions + entry.getValue().size();
        }
        this.partitionNum = partitions;
    }

    public CuboidToPartitionMapping(CubeSegment cubeSeg, KylinConfig kylinConfig) throws IOException {
        cuboidPartitions = Maps.newHashMap();

        Set<Long> allCuboidIds = cubeSeg.getCuboidScheduler().getAllCuboidIds();

        CalculatePartitionId(cubeSeg, kylinConfig, allCuboidIds);
    }

    public CuboidToPartitionMapping(CubeSegment cubeSeg, KylinConfig kylinConfig, int level) throws IOException {
        cuboidPartitions = Maps.newHashMap();

        List<Long> layeredCuboids = cubeSeg.getCuboidScheduler().getCuboidsByLayer().get(level);

        CalculatePartitionId(cubeSeg, kylinConfig, layeredCuboids);
    }

    private void CalculatePartitionId(CubeSegment cubeSeg, KylinConfig kylinConfig, Collection<Long> cuboidIds) throws IOException {
        int position = 0;
        CubeStatsReader cubeStatsReader = new CubeStatsReader(cubeSeg, kylinConfig);
        for (Long cuboidId : cuboidIds) {
            int partition = estimateCuboidPartitionNum(cuboidId, cubeStatsReader, kylinConfig);
            List<Integer> positions = Lists.newArrayListWithCapacity(partition);

            for (int i = position; i < position + partition; i++) {
                positions.add(i);
            }

            cuboidPartitions.put(cuboidId, positions);
            position = position + partition;
        }

        this.partitionNum = position;
    }

    public String serialize() throws JsonProcessingException {
        return JsonUtil.writeValueAsString(cuboidPartitions);
    }

    public static CuboidToPartitionMapping deserialize(String jsonMapping) throws IOException {
        Map<Long, List<Integer>> cuboidPartitions = JsonUtil.readValue(jsonMapping, new TypeReference<Map<Long, List<Integer>>>() {});
        return new CuboidToPartitionMapping(cuboidPartitions);
    }

    public int getNumPartitions() {
        return this.partitionNum;
    }

    public long getCuboidIdByPartition(int partition) {
        for (Map.Entry<Long, List<Integer>> entry : cuboidPartitions.entrySet()) {
            if (entry.getValue().contains(partition)) {
                return entry.getKey();
            }
        }

        throw new IllegalArgumentException("No cuboidId for partition id: " + partition);
    }

    public int getPartitionByKey(byte[] key) {
        long cuboidId = Bytes.toLong(key, RowConstants.ROWKEY_SHARDID_LEN, RowConstants.ROWKEY_CUBOIDID_LEN);
        List<Integer> partitions = cuboidPartitions.get(cuboidId);
        int partitionKey = mod(key, RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN, key.length, partitions.size());

        return partitions.get(partitionKey);
    }

    private int mod(byte[] src, int start, int end, int total) {
        int sum = Bytes.hashBytes(src, start, end - start);
        int mod = sum % total;
        if (mod < 0)
            mod += total;

        return mod;
    }

    public int getPartitionNumForCuboidId(long cuboidId) {
        return cuboidPartitions.get(cuboidId).size();
    }

    public String getPartitionFilePrefix(int partition) {
        long cuboid = getCuboidIdByPartition(partition);
        int partNum = partition % getPartitionNumForCuboidId(cuboid);
        String prefix = ParquetJobSteps.getCuboidOutputFileName(cuboid, partNum);

        return prefix;
    }

    private int estimateCuboidPartitionNum(long cuboidId, CubeStatsReader cubeStatsReader, KylinConfig kylinConfig) {
        double cuboidSize = cubeStatsReader.estimateCuboidSize(cuboidId);
        float rddCut = kylinConfig.getParquetFileSizeMB();
        int partition = (int) (cuboidSize / rddCut);
        partition = Math.max(kylinConfig.getParquetMinPartitions(), partition);
        partition = Math.min(kylinConfig.getParquetMaxPartitions(), partition);

        logger.info("cuboid:{}, est_size:{}, partitions:{}", cuboidId, cuboidSize, partition);
        return partition;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, List<Integer>> entry : cuboidPartitions.entrySet()) {
            sb.append("cuboidId:").append(entry.getKey()).append(" [").append(StringUtils.join(entry.getValue(), ",")).append("]\n");
        }

        return sb.toString();
    }
}

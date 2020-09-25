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

package org.apache.kylin.storage.hbase.steps;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.CubeStatsReader;
import org.apache.kylin.engine.mr.common.CuboidShardUtil;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

/**
 */
public class CreateHTableJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(CreateHTableJob.class);

    CubeInstance cube = null;
    CubeDesc cubeDesc = null;
    String segmentID = null;
    String cuboidModeName = null;
    String hbaseConfPath = null;
    KylinConfig kylinConfig;
    Path partitionFilePath;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_PARTITION_FILE_PATH);
        options.addOption(OPTION_CUBOID_MODE);
        options.addOption(OPTION_HBASE_CONF_PATH);
        parseOptions(options, args);

        partitionFilePath = new Path(getOptionValue(OPTION_PARTITION_FILE_PATH));

        String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase(Locale.ROOT);
        CubeManager cubeMgr = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());
        cube = cubeMgr.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        kylinConfig = cube.getConfig();
        segmentID = getOptionValue(OPTION_SEGMENT_ID);
        cuboidModeName = getOptionValue(OPTION_CUBOID_MODE);
        hbaseConfPath = getOptionValue(OPTION_HBASE_CONF_PATH);
        CubeSegment cubeSegment = cube.getSegmentById(segmentID);

        byte[][] splitKeys;
        Map<Long, Double> cuboidSizeMap = new CubeStatsReader(cubeSegment, kylinConfig).getCuboidSizeMap();

        // for cube planner, will keep cuboidSizeMap unchanged if cube planner is disabled
        Set<Long> buildingCuboids = cube.getCuboidsByMode(cuboidModeName);
        if (buildingCuboids != null && !buildingCuboids.isEmpty()) {
            Map<Long, Double> optimizedCuboidSizeMap = Maps.newHashMapWithExpectedSize(buildingCuboids.size());
            for (Long cuboid : buildingCuboids) {
                Double cuboidSize = cuboidSizeMap.get(cuboid);
                if (cuboidSize == null) {
                    logger.warn("{} cuboid's size is null will replace by 0", cuboid);
                    cuboidSize = 0.0;
                }
                optimizedCuboidSizeMap.put(cuboid, cuboidSize);
            }
            cuboidSizeMap = optimizedCuboidSizeMap;
        }

        splitKeys = getRegionSplitsFromCuboidStatistics(cuboidSizeMap, kylinConfig, cubeSegment,
                partitionFilePath.getParent());

        CubeHTableUtil.createHTable(cubeSegment, splitKeys);

        // export configuration in advance to avoid connecting to hbase from spark
        if (cubeDesc.getEngineType()== IEngineAware.ID_SPARK){
            exportHBaseConfiguration(cubeSegment.getStorageLocationIdentifier());
        }
        return 0;
    }

    private void exportHBaseConfiguration(String hbaseTableName) throws IOException {

        Configuration hbaseConf = HBaseConnection.getCurrentHBaseConfiguration();
        HadoopUtil.healSickConfig(hbaseConf);
        Job job = Job.getInstance(hbaseConf, hbaseTableName);
        HTable table = new HTable(hbaseConf, hbaseTableName);
        HFileOutputFormat3.configureIncrementalLoadMap(job, table);

        logger.info("Saving HBase configuration to {}", hbaseConfPath);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FSDataOutputStream out = null;
        try {
            out = fs.create(new Path(hbaseConfPath));
            job.getConfiguration().writeXml(out);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    //one region for one shard
    private static byte[][] getSplitsByRegionCount(int regionCount) {
        byte[][] result = new byte[regionCount - 1][];
        for (int i = 1; i < regionCount; ++i) {
            byte[] split = new byte[RowConstants.ROWKEY_SHARDID_LEN];
            BytesUtil.writeUnsigned(i, split, 0, RowConstants.ROWKEY_SHARDID_LEN);
            result[i - 1] = split;
        }
        return result;
    }

    public static byte[][] getRegionSplitsFromCuboidStatistics(final Map<Long, Double> cubeSizeMap,
            final KylinConfig kylinConfig, final CubeSegment cubeSegment, final Path hfileSplitsOutputFolder)
            throws IOException {

        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        float cut = cubeDesc.getConfig().getKylinHBaseRegionCut();

        logger.info("Cut for HBase region is {} GB", cut);

        double totalSizeInM = 0;
        for (Double cuboidSize : cubeSizeMap.values()) {
            totalSizeInM += cuboidSize;
        }

        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cubeSizeMap.keySet());
        Collections.sort(allCuboids);

        int nRegion = Math.round((float) (totalSizeInM / (cut * 1024L)));
        nRegion = Math.max(kylinConfig.getHBaseRegionCountMin(), nRegion);
        nRegion = Math.min(kylinConfig.getHBaseRegionCountMax(), nRegion);

        if (cubeSegment.isEnableSharding()) {
            //use prime nRegions to help random sharding
            int original = nRegion;
            if (nRegion == 0) {
                nRegion = 1;
            }

            if (nRegion > Short.MAX_VALUE) {
                logger.info("Too many regions! reduce to {}", Short.MAX_VALUE);
                nRegion = Short.MAX_VALUE;
            }

            if (nRegion != original) {
                logger.info("Region count is adjusted from {} to {} to help random sharding", original, nRegion);
            }
        }

        int mbPerRegion = (int) (totalSizeInM / nRegion);
        mbPerRegion = Math.max(1, mbPerRegion);

        logger.info("Total size {} M (estimated)", totalSizeInM);
        logger.info("Expecting {} regions.", nRegion);
        logger.info("Expecting {} MB per region.", mbPerRegion);

        if (cubeSegment.isEnableSharding()) {
            //each cuboid will be split into different number of shards
            HashMap<Long, Short> cuboidShards = Maps.newHashMap();

            //each shard/region may be split into multiple hfiles; array index: region ID, Map: key: cuboidID, value cuboid size in the region
            List<HashMap<Long, Double>> innerRegionSplits = Lists.newArrayList();
            for (int i = 0; i < nRegion; i++) {
                innerRegionSplits.add(new HashMap<Long, Double>());
            }

            double[] regionSizes = new double[nRegion];
            for (long cuboidId : allCuboids) {
                double estimatedSize = cubeSizeMap.get(cuboidId);
                double magic = 23;
                int shardNum = (int) (estimatedSize * magic / mbPerRegion + 1);
                if (shardNum < 1) {
                    shardNum = 1;
                }

                if (shardNum > nRegion) {
                    logger.debug(String.format(Locale.ROOT,
                            "Cuboid %d 's estimated size %.2f MB will generate %d regions, " + "reduce to %d", cuboidId,
                            estimatedSize, shardNum, nRegion));
                    shardNum = nRegion;
                } else {
                    logger.debug(
                            String.format(Locale.ROOT, "Cuboid %d 's estimated size %.2f MB will generate %d regions",
                                    cuboidId, estimatedSize, shardNum));
                }

                cuboidShards.put(cuboidId, (short) shardNum);
                short startShard = ShardingHash.getShard(cuboidId, nRegion);
                for (short i = startShard; i < startShard + shardNum; ++i) {
                    short j = (short) (i % nRegion);
                    regionSizes[j] = regionSizes[j] + estimatedSize / shardNum;
                    innerRegionSplits.get(j).put(cuboidId, estimatedSize / shardNum);
                }
            }

            for (int i = 0; i < nRegion; ++i) {
                logger.debug("Region {}'s estimated size is {} MB, accounting for {} percent", i, regionSizes[i],
                        100.0 * regionSizes[i] / totalSizeInM);
            }

            CuboidShardUtil.saveCuboidShards(cubeSegment, cuboidShards, nRegion);
            saveHFileSplits(innerRegionSplits, mbPerRegion, hfileSplitsOutputFolder, kylinConfig);
            return getSplitsByRegionCount(nRegion);
        } else {
            throw new IllegalStateException("Not supported");
        }
    }

    protected static void saveHFileSplits(final List<HashMap<Long, Double>> innerRegionSplits, int mbPerRegion,
            final Path outputFolder, final KylinConfig kylinConfig) throws IOException {

        if (outputFolder == null) {
            logger.warn("outputFolder for hfile split file is null, skip inner region split");
            return;
        }

        // note read-write separation, respect HBase FS here
        Configuration hbaseConf = HBaseConnection.getCurrentHBaseConfiguration();
        FileSystem fs = HadoopUtil.getFileSystem(outputFolder, hbaseConf);
        if (fs.exists(outputFolder) == false) {
            fs.mkdirs(outputFolder);
        }

        final float hfileSizeGB = kylinConfig.getHBaseHFileSizeGB();
        float hfileSizeMB = hfileSizeGB * 1024;
        if (hfileSizeMB > mbPerRegion) {
            hfileSizeMB = mbPerRegion;
        }

        // keep the tweak for sandbox test
        if (hfileSizeMB > 0.0f && kylinConfig.isDevEnv()) {
            hfileSizeMB = mbPerRegion / 2f;
        }

        int compactionThreshold = Integer.parseInt(hbaseConf.get("hbase.hstore.compactionThreshold", "3"));
        logger.info("hbase.hstore.compactionThreshold is {}", compactionThreshold);
        if (hfileSizeMB > 0.0f && hfileSizeMB * compactionThreshold < mbPerRegion) {
            hfileSizeMB = ((float) mbPerRegion) / compactionThreshold;
        }

        if (hfileSizeMB <= 0f) {
            hfileSizeMB = mbPerRegion;
        }
        logger.info("hfileSizeMB {}", hfileSizeMB);
        final Path hfilePartitionFile = new Path(outputFolder, "part-r-00000_hfile");
        short regionCount = (short) innerRegionSplits.size();

        List<byte[]> splits = Lists.newArrayList();
        for (int i = 0; i < regionCount; i++) {
            if (i > 0) {
                // skip 0
                byte[] split = new byte[RowConstants.ROWKEY_SHARDID_LEN];
                BytesUtil.writeUnsigned(i, split, 0, RowConstants.ROWKEY_SHARDID_LEN);
                splits.add(split);
            }

            HashMap<Long, Double> cuboidSize = innerRegionSplits.get(i);
            List<Long> allCuboids = Lists.newArrayList();
            allCuboids.addAll(cuboidSize.keySet());
            Collections.sort(allCuboids);

            double accumulatedSize = 0;
            int j = 0;
            for (Long cuboid : allCuboids) {

                if (accumulatedSize >= hfileSizeMB) {
                    logger.debug("Region {}'s hfile {} size is {} mb", i, j, accumulatedSize);
                    byte[] split = new byte[RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN];
                    BytesUtil.writeUnsigned(i, split, 0, RowConstants.ROWKEY_SHARDID_LEN);
                    System.arraycopy(Bytes.toBytes(cuboid), 0, split, RowConstants.ROWKEY_SHARDID_LEN,
                            RowConstants.ROWKEY_CUBOIDID_LEN);
                    splits.add(split);
                    accumulatedSize = 0;
                    j++;
                }
                accumulatedSize += cuboidSize.get(cuboid);
            }

        }

        try (SequenceFile.Writer hfilePartitionWriter = SequenceFile.createWriter(hbaseConf,
                SequenceFile.Writer.file(hfilePartitionFile), SequenceFile.Writer.keyClass(RowKeyWritable.class),
                SequenceFile.Writer.valueClass(NullWritable.class))) {

            for (int i = 0; i < splits.size(); i++) {
                //when we compare the rowkey, we compare the row firstly.
                hfilePartitionWriter.append(
                        new RowKeyWritable(KeyValueUtil.createFirstOnRow(splits.get(i), 9223372036854775807L).createKeyOnly(false).getKey()),
                        NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateHTableJob(), args);
        System.exit(exitCode);
    }
}

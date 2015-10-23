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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class CreateHTableJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(CreateHTableJob.class);

    CubeInstance cube = null;
    CubeDesc cubeDesc = null;
    String segmentName = null;
    KylinConfig kylinConfig;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_NAME);
        options.addOption(OPTION_PARTITION_FILE_PATH);
        options.addOption(OPTION_HTABLE_NAME);
        options.addOption(OPTION_STATISTICS_ENABLED);
        parseOptions(options, args);

        Path partitionFilePath = new Path(getOptionValue(OPTION_PARTITION_FILE_PATH));
        boolean statistics_enabled = Boolean.parseBoolean(getOptionValue(OPTION_STATISTICS_ENABLED));

        String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(kylinConfig);
        cube = cubeMgr.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        segmentName = getOptionValue(OPTION_SEGMENT_NAME);
        CubeSegment cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

        String tableName = getOptionValue(OPTION_HTABLE_NAME).toUpperCase();
        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();

        try {

            byte[][] splitKeys;
            if (statistics_enabled) {
                final Map<Long, Long> cuboidSizeMap = getCubeRowCountMapFromCuboidStatistics(cubeSegment, kylinConfig, conf);
                splitKeys = getSplitsFromCuboidStatistics(cuboidSizeMap, kylinConfig, cubeSegment);
            } else {
                splitKeys = getSplits(conf, partitionFilePath);
            }

            CubeHTableUtil.createHTable(cubeDesc, tableName, splitKeys);
            return 0;
        } catch (Exception e) {
            printUsage(options);
            e.printStackTrace(System.err);
            logger.error(e.getLocalizedMessage(), e);
            return 2;
        }
    }

    @SuppressWarnings("deprecation")
    public byte[][] getSplits(Configuration conf, Path path) throws Exception {
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path) == false) {
            System.err.println("Path " + path + " not found, no region split, HTable will be one region");
            return null;
        }

        List<byte[]> rowkeyList = new ArrayList<byte[]>();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                rowkeyList.add(((Text) key).copyBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            IOUtils.closeStream(reader);
        }

        logger.info((rowkeyList.size() + 1) + " regions");
        logger.info(rowkeyList.size() + " splits");
        for (byte[] split : rowkeyList) {
            logger.info(StringUtils.byteToHexString(split));
        }

        byte[][] retValue = rowkeyList.toArray(new byte[rowkeyList.size()][]);
        return retValue.length == 0 ? null : retValue;
    }

    public static Map<Long, Long> getCubeRowCountMapFromCuboidStatistics(CubeSegment cubeSegment, KylinConfig kylinConfig, Configuration conf) throws IOException {
        ResourceStore rs = ResourceStore.getStore(kylinConfig);
        String fileKey = cubeSegment.getStatisticsResourcePath();
        InputStream is = rs.getResource(fileKey);
        File tempFile = null;
        FileOutputStream tempFileStream = null;
        try {
            tempFile = File.createTempFile(cubeSegment.getUuid(), ".seq");
            tempFileStream = new FileOutputStream(tempFile);
            org.apache.commons.io.IOUtils.copy(is, tempFileStream);
        } finally {
            IOUtils.closeStream(is);
            IOUtils.closeStream(tempFileStream);
        }
        Map<Long, HyperLogLogPlusCounter> counterMap = Maps.newHashMap();
        
        FileSystem fs = HadoopUtil.getFileSystem("file:///" + tempFile.getAbsolutePath());
        int samplingPercentage = 25;
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, new Path(tempFile.getAbsolutePath()), conf);
            LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                if (key.get() == 0L) {
                    samplingPercentage = Bytes.toInt(value.getBytes());
                } else {
                    HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(14);
                    ByteArray byteArray = new ByteArray(value.getBytes());
                    hll.readRegisters(byteArray.asBuffer());
                    counterMap.put(key.get(), hll);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            IOUtils.closeStream(reader);
            tempFile.delete();
        }
        return getCubeRowCountMapFromCuboidStatistics(counterMap, samplingPercentage);
    }

    public static Map<Long, Long> getCubeRowCountMapFromCuboidStatistics(Map<Long, HyperLogLogPlusCounter> counterMap, final int samplingPercentage) throws IOException {
        Preconditions.checkArgument(samplingPercentage > 0);
        return Maps.transformValues(counterMap, new Function<HyperLogLogPlusCounter, Long>() {
            @Nullable
            @Override
            public Long apply(HyperLogLogPlusCounter input) {
                return input.getCountEstimate() * 100 / samplingPercentage;
            }
        });
    }

    public static byte[][] getSplitsFromCuboidStatistics(final Map<Long, Long> cubeRowCountMap, KylinConfig kylinConfig, CubeSegment cubeSegment) throws IOException {

        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();

        final List<Integer> rowkeyColumnSize = Lists.newArrayList();
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        List<TblColRef> columnList = baseCuboid.getColumns();

        for (int i = 0; i < columnList.size(); i++) {
            logger.info("Rowkey column " + i + " length " + cubeSegment.getColumnLength(columnList.get(i)));
            rowkeyColumnSize.add(cubeSegment.getColumnLength(columnList.get(i)));
        }

        DataModelDesc.RealizationCapacity cubeCapacity = cubeDesc.getModel().getCapacity();
        int cut = kylinConfig.getHBaseRegionCut(cubeCapacity.toString());

        logger.info("Cube capacity " + cubeCapacity.toString() + ", chosen cut for HTable is " + cut + "GB");

        long totalSizeInM = 0;

        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cubeRowCountMap.keySet());
        Collections.sort(allCuboids);

        Map<Long, Long> cubeSizeMap = Maps.transformEntries(cubeRowCountMap, new Maps.EntryTransformer<Long, Long, Long>() {
            @Override
            public Long transformEntry(@Nullable Long key, @Nullable Long value) {
                return estimateCuboidStorageSize(cubeDesc, key, value, baseCuboidId, rowkeyColumnSize);
            }
        });
        for (Long cuboidSize : cubeSizeMap.values()) {
            totalSizeInM += cuboidSize;
        }

        int nRegion = Math.round((float) totalSizeInM / (cut * 1024L));
        nRegion = Math.max(kylinConfig.getHBaseRegionCountMin(), nRegion);
        nRegion = Math.min(kylinConfig.getHBaseRegionCountMax(), nRegion);

        int mbPerRegion = (int) (totalSizeInM / (nRegion));
        mbPerRegion = Math.max(1, mbPerRegion);

        logger.info("Total size " + totalSizeInM + "M (estimated)");
        logger.info(nRegion + " regions (estimated)");
        logger.info(mbPerRegion + " MB per region (estimated)");

        List<Long> regionSplit = Lists.newArrayList();

        long size = 0;
        int regionIndex = 0;
        int cuboidCount = 0;
        for (int i = 0; i < allCuboids.size(); i++) {
            long cuboidId = allCuboids.get(i);
            if (size >= mbPerRegion || (size + cubeSizeMap.get(cuboidId)) >= mbPerRegion * 1.2) {
                // if the size already bigger than threshold, or it will exceed by 20%, cut for next region
                regionSplit.add(cuboidId);
                logger.info("Region " + regionIndex + " will be " + size + " MB, contains cuboids < " + cuboidId + " (" + cuboidCount + ") cuboids");
                size = 0;
                cuboidCount = 0;
                regionIndex++;
            }
            size += cubeSizeMap.get(cuboidId);
            cuboidCount++;
        }

        byte[][] result = new byte[regionSplit.size()][];
        for (int i = 0; i < regionSplit.size(); i++) {
            result[i] = Bytes.toBytes(regionSplit.get(i));
        }

        return result;
    }

    /**
     * Estimate the cuboid's size
     *
     * @param cubeDesc
     * @param cuboidId
     * @param rowCount
     * @return the cuboid size in M bytes
     */
    private static long estimateCuboidStorageSize(CubeDesc cubeDesc, long cuboidId, long rowCount, long baseCuboidId, List<Integer> rowKeyColumnLength) {

        int bytesLength = RowConstants.ROWKEY_CUBOIDID_LEN;

        long mask = Long.highestOneBit(baseCuboidId);
        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(baseCuboidId);
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & cuboidId) > 0) {
                bytesLength += rowKeyColumnLength.get(i); //colIO.getColumnLength(columnList.get(i));
            }
            mask = mask >> 1;
        }

        // add the measure length
        int space = 0;
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            DataType returnType = measureDesc.getFunction().getReturnDataType();
            if (returnType.isHLLC()) {
                // for HLL, it will be compressed when export to bytes
                space += returnType.getStorageBytesEstimate() * 0.75;
            } else {
                space += returnType.getStorageBytesEstimate();
            }
        }
        bytesLength += space;

        logger.info("Cuboid " + cuboidId + " has " + rowCount + " rows, each row size is " + bytesLength + " bytes.");
        logger.info("Cuboid " + cuboidId + " total size is " + (bytesLength * rowCount / (1024L * 1024L)) + "M.");
        return bytesLength * rowCount / (1024L * 1024L);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateHTableJob(), args);
        System.exit(exitCode);
    }
}

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

package org.apache.kylin.job.hadoop.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.job.tools.DeployCoprocessorCLI;
import org.apache.kylin.job.tools.LZOSupportnessChecker;
import org.apache.kylin.metadata.model.*;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author George Song (ysong1)
 */

public class CreateHTableJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(CreateHTableJob.class);

    public static final int MAX_REGION = 1000;

    CubeInstance cube = null;
    CubeDesc cubeDesc = null;
    String segmentName = null;
    KylinConfig kylinConfig;
    private int SAMPING_PERCENTAGE = 5;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_NAME);
        options.addOption(OPTION_PARTITION_FILE_PATH);
        options.addOption(OPTION_HTABLE_NAME);
        options.addOption(OPTION_STATISTICS_ENABLED);
        options.addOption(OPTION_STATISTICS_SAMPLING_PERCENT);
        parseOptions(options, args);

        Path partitionFilePath = new Path(getOptionValue(OPTION_PARTITION_FILE_PATH));
        boolean statistics_enabled = Boolean.parseBoolean(getOptionValue(OPTION_STATISTICS_ENABLED));

        String statistics_sampling_percent = getOptionValue(OPTION_STATISTICS_SAMPLING_PERCENT);

        SAMPING_PERCENTAGE = Integer.parseInt(statistics_sampling_percent);

        String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
        kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(kylinConfig);
        cube = cubeMgr.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        segmentName = getOptionValue(OPTION_SEGMENT_NAME);

        String tableName = getOptionValue(OPTION_HTABLE_NAME).toUpperCase();
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        // https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/regionserver/ConstantSizeRegionSplitPolicy.html
        tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
        tableDesc.setValue(IRealizationConstants.HTableTag, kylinConfig.getMetadataUrlPrefix());

        Configuration conf = HBaseConfiguration.create(getConf());
        HBaseAdmin admin = new HBaseAdmin(conf);

        try {
            if (User.isHBaseSecurityEnabled(conf)) {
                // add coprocessor for bulk load
                tableDesc.addCoprocessor("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
            }

            for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
                HColumnDescriptor cf = new HColumnDescriptor(cfDesc.getName());
                cf.setMaxVersions(1);

                if (LZOSupportnessChecker.getSupportness()) {
                    logger.info("hbase will use lzo to compress cube data");
                    cf.setCompressionType(Algorithm.LZO);
                } else {
                    logger.info("hbase will not use lzo to compress cube data");
                }

                cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                cf.setInMemory(false);
                cf.setBlocksize(4 * 1024 * 1024); // set to 4MB
                tableDesc.addFamily(cf);
            }

            byte[][] splitKeys;
            if (statistics_enabled) {
                splitKeys = getSplitsFromCuboidStatistics(conf);
            } else {
                splitKeys = getSplits(conf, partitionFilePath);
            }

            if (admin.tableExists(tableName)) {
                // admin.disableTable(tableName);
                // admin.deleteTable(tableName);
                throw new RuntimeException("HBase table " + tableName + " exists!");
            }

            DeployCoprocessorCLI.deployCoprocessor(tableDesc);

            admin.createTable(tableDesc, splitKeys);
            logger.info("create hbase table " + tableName + " done.");

            return 0;
        } catch (Exception e) {
            printUsage(options);
            e.printStackTrace(System.err);
            logger.error(e.getLocalizedMessage(), e);
            return 2;
        } finally {
            admin.close();
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


    @SuppressWarnings("deprecation")
    protected byte[][] getSplitsFromCuboidStatistics(Configuration conf) throws IOException {

        List<Integer> rowkeyColumnSize = Lists.newArrayList();
        CubeSegment cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        List<TblColRef> columnList = baseCuboid.getColumns();

        for (int i = 0; i < columnList.size(); i++) {
            logger.info("Rowkey column " + i + " length " + cubeSegment.getColumnLength(columnList.get(i)));
            rowkeyColumnSize.add(cubeSegment.getColumnLength(columnList.get(i)));
        }

        DataModelDesc.RealizationCapacity cubeCapacity = cubeDesc.getModel().getCapacity();
        int cut = kylinConfig.getHBaseRegionCut(cubeCapacity.toString());

        logger.info("Cube capacity " + cubeCapacity.toString() + ", chosen cut for HTable is " + cut + "GB");

        Map<Long, Long> cuboidSizeMap = Maps.newHashMap();
        long totalSizeInM = 0;

        ResourceStore rs = ResourceStore.getStore(kylinConfig);
        String fileKey = ResourceStore.CUBE_STATISTICS_ROOT + "/" + cube.getName() + "/" + cubeSegment.getUuid() + ".seq";
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

        FileSystem fs = HadoopUtil.getFileSystem("file:///" +tempFile.getAbsolutePath());
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, new Path(tempFile.getAbsolutePath()), conf);
            LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(16);
                ByteArray byteArray = new ByteArray(value.getBytes());
                hll.readRegisters(byteArray.asBuffer());

                cuboidSizeMap.put(key.get(), hll.getCountEstimate() * 100 / SAMPING_PERCENTAGE);

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            IOUtils.closeStream(reader);
        }

        List<Long> allCuboids = Lists.newArrayList();
        allCuboids.addAll(cuboidSizeMap.keySet());
        Collections.sort(allCuboids);

        for (long cuboidId : allCuboids) {
            long cuboidSize = estimateCuboidStorageSize(cuboidId, cuboidSizeMap.get(cuboidId), baseCuboidId, rowkeyColumnSize);
            cuboidSizeMap.put(cuboidId, cuboidSize);
            totalSizeInM += cuboidSize;
        }

        int nRegion = Math.round((float) totalSizeInM / ((float) cut * 1024l));
        nRegion = Math.max(1, nRegion);
        nRegion = Math.min(MAX_REGION, nRegion);

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
            if (size >= mbPerRegion || (size + cuboidSizeMap.get(cuboidId)) >= mbPerRegion * 1.2) {
                // if the size already bigger than threshold, or it will exceed by 20%, cut for next region
                regionSplit.add(cuboidId);
                logger.info("Region " + regionIndex + " will be " + size + " MB, contains cuboids < " + cuboidId + " (" + cuboidCount + ") cuboids");
                size = 0;
                cuboidCount = 0;
                regionIndex++;
            }
            size += cuboidSizeMap.get(cuboidId);
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
     * @param cuboidId
     * @param rowCount
     * @return the cuboid size in M bytes
     */
    private long estimateCuboidStorageSize(long cuboidId, long rowCount, long baseCuboidId, List<Integer> rowKeyColumnLength) {

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
                space += returnType.getSpaceEstimate() * 0.75;
            } else {
                space += returnType.getSpaceEstimate();
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

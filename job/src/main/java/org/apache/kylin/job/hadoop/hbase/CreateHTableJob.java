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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.job.tools.DeployCoprocessorCLI;
import org.apache.kylin.job.tools.LZOSupportnessChecker;
import org.apache.kylin.metadata.model.*;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author George Song (ysong1)
 */

public class CreateHTableJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(CreateHTableJob.class);
    public static final int SMALL_CUT = 5;  //  5 GB per region
    public static final int MEDIUM_CUT = 10; //  10 GB per region
    public static final int LARGE_CUT = 50; // 50 GB per region

    public static final int MAX_REGION = 1000;

    CubeInstance cube = null;
    CubeDesc cubeDesc = null;
    String segmentName = null;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_NAME);
        options.addOption(OPTION_PARTITION_FILE_PATH);
        options.addOption(OPTION_HTABLE_NAME);
        options.addOption(OPTION_STATISTICS_ENABLED);
        options.addOption(OPTION_STATISTICS_OUTPUT);
        parseOptions(options, args);

        Path partitionFilePath = new Path(getOptionValue(OPTION_PARTITION_FILE_PATH));
        boolean statistics_enabled = Boolean.parseBoolean(getOptionValue(OPTION_STATISTICS_ENABLED));
        Path statisticsFilePath = new Path(getOptionValue(OPTION_STATISTICS_OUTPUT), BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION);

        String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        cube = cubeMgr.getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        segmentName = getOptionValue(OPTION_SEGMENT_NAME);

        String tableName = getOptionValue(OPTION_HTABLE_NAME).toUpperCase();
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        // https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/regionserver/ConstantSizeRegionSplitPolicy.html
        tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
        tableDesc.setValue(IRealizationConstants.HTableTag, config.getMetadataUrlPrefix());

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
                    logger.info("hbase will use lzo to compress data");
                    cf.setCompressionType(Algorithm.LZO);
                } else {
                    logger.info("hbase will not use lzo to compress data");
                }

                cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                cf.setInMemory(false);
                cf.setBlocksize(4 * 1024 * 1024); // set to 4MB
                tableDesc.addFamily(cf);
            }

            byte[][] splitKeys;
            if (statistics_enabled) {
                splitKeys = getSplitsFromCuboidStatistics(conf, statisticsFilePath);
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
            System.out.println(StringUtils.byteToHexString(split));
        }

        byte[][] retValue = rowkeyList.toArray(new byte[rowkeyList.size()][]);
        return retValue.length == 0 ? null : retValue;
    }


    @SuppressWarnings("deprecation")
    protected byte[][] getSplitsFromCuboidStatistics(Configuration conf, Path statisticsFilePath) throws IOException {

        List<Integer> rowkeyColumnSize = Lists.newArrayList();
        CubeSegment cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.NEW);
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        List<TblColRef> columnList = baseCuboid.getColumns();

        for (int i = 0; i < columnList.size(); i++) {
            System.out.println("Rowkey column " + i + " length " + cubeSegment.getColumnLength(columnList.get(i)));
            rowkeyColumnSize.add(cubeSegment.getColumnLength(columnList.get(i)));
        }

        DataModelDesc.RealizationCapacity cubeCapacity = cubeDesc.getModel().getCapacity();
        int cut;
        switch (cubeCapacity) {
            case SMALL:
                cut = SMALL_CUT;
                break;
            case MEDIUM:
                cut = MEDIUM_CUT;
                break;
            case LARGE:
                cut = LARGE_CUT;
                break;
            default:
                cut = SMALL_CUT;
        }

        System.out.println("Chosen cut for htable is " + cut);

        Map<Long, Long> cuboidSizeMap = Maps.newHashMap();
        long totalSizeInM = 0;

        SequenceFile.Reader reader = null;

        FileSystem fs = statisticsFilePath.getFileSystem(conf);
        if (fs.exists(statisticsFilePath) == false) {
            System.err.println("Path " + statisticsFilePath + " not found, no region split, HTable will be one region");
            return null;
        }

        try {
            reader = new SequenceFile.Reader(fs, statisticsFilePath, conf);
            LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            LongWritable value = (LongWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                cuboidSizeMap.put(key.get(), value.get());
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

        for (long i : allCuboids) {
            long cuboidSize = estimateCuboidStorageSize(i, cuboidSizeMap.get(i), baseCuboidId, rowkeyColumnSize);
            cuboidSizeMap.put(i, cuboidSize);
            totalSizeInM += cuboidSize;
        }

        int nRegion = Math.round((float) totalSizeInM / ((float) cut) * 1024l);
        nRegion = Math.max(1, nRegion);
        nRegion = Math.min(MAX_REGION, nRegion);

        int gbPerRegion = (int) (totalSizeInM / (nRegion * 1024l));
        gbPerRegion = Math.max(1, gbPerRegion);

        System.out.println("Total size " + totalSizeInM + "M");
        System.out.println(nRegion + " regions");
        System.out.println(gbPerRegion + " GB per region");

        List<Long> regionSplit = Lists.newArrayList();

        long size = 0;
        int regionIndex = 0;
        for (long cuboidId : allCuboids) {
            size += cuboidSizeMap.get(cuboidId);
            if (size >= gbPerRegion * 1024l) {
                regionSplit.add(cuboidId);
                System.out.println("Region " + regionIndex + " will be " + size + " MB, contains cuboid to " + cuboidId);
                size = 0;
                regionIndex++;
            }
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

        System.out.println("Cuboid " + cuboidId + " has " + rowCount + " rows, each row size is " + bytesLength);
        System.out.println("Cuboid " + cuboidId + " total size is " + (bytesLength * rowCount / (1024L * 1024L)) + "M");
        return bytesLength * rowCount / (1024L * 1024L);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateHTableJob(), args);
        System.exit(exitCode);
    }
}

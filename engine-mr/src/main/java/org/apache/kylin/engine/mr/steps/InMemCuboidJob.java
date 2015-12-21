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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
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
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.ByteArrayWritable;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
public class InMemCuboidJob extends AbstractHadoopJob {

    protected static final Logger logger = LoggerFactory.getLogger(InMemCuboidJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_SEGMENT_NAME);
            options.addOption(OPTION_OUTPUT_PATH);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
            String segmentName = getOptionValue(OPTION_SEGMENT_NAME);
            String output = getOptionValue(OPTION_OUTPUT_PATH);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(config);
            CubeInstance cube = cubeMgr.getCube(cubeName);
            CubeSegment cubeSeg = cube.getSegment(segmentName, SegmentStatusEnum.NEW);

            job = Job.getInstance(getConf(), getOptionValue(OPTION_JOB_NAME));
            logger.info("Starting: " + job.getJobName());

            setJobClasspath(job);

            // add metadata to distributed cache
            attachKylinPropsAndMetadata(cube, job.getConfiguration());

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);
            long timeout = 1000 * 60 * 60L; // 1 hour
            job.getConfiguration().set("mapred.task.timeout", String.valueOf(timeout));

            // set input
            IMRTableInputFormat flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();
            flatTableInputFormat.configureJob(job);

            // set mapper
            job.setMapperClass(InMemCuboidMapper.class);
            job.setMapOutputKeyClass(ByteArrayWritable.class);
            job.setMapOutputValueClass(ByteArrayWritable.class);

            // set output
            job.setReducerClass(InMemCuboidReducer.class);
            job.setNumReduceTasks(calculateReducerNum(cubeSeg));

            // the cuboid file and KV class must be compatible with 0.7 version for smooth upgrade
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            Path outputPath = new Path(output);
            FileOutputFormat.setOutputPath(job, outputPath);

            HadoopUtil.deletePath(job.getConfiguration(), outputPath);
            
            return waitForCompletion(job);
        } catch (Exception e) {
            logger.error("error in CuboidJob", e);
            printUsage(options);
            throw e;
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private int calculateReducerNum(CubeSegment cubeSeg) throws IOException {
        Configuration jobConf = job.getConfiguration();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        
        Map<Long, Double> cubeSizeMap = getCubeSizeMapFromCuboidStatistics(cubeSeg, kylinConfig, jobConf);
        double totalSizeInM = 0;
        for (Double cuboidSize : cubeSizeMap.values()) {
            totalSizeInM += cuboidSize;
        }
        
        double perReduceInputMB = kylinConfig.getDefaultHadoopJobReducerInputMB();
        
        // number of reduce tasks
        int numReduceTasks = (int) Math.round(totalSizeInM / perReduceInputMB);

        // at least 1 reducer
        numReduceTasks = Math.max(1, numReduceTasks);
        // no more than 5000 reducer by default
        numReduceTasks = Math.min(kylinConfig.getHadoopJobMaxReducerNumber(), numReduceTasks);

        logger.info("Having total map input MB " + Math.round(totalSizeInM));
        logger.info("Having per reduce MB " + perReduceInputMB);
        logger.info("Setting " + "mapred.reduce.tasks" + "=" + numReduceTasks);
        return numReduceTasks;
    }

    public static Map<Long, Long> getCubeRowCountMapFromCuboidStatistics(CubeSegment cubeSegment, KylinConfig kylinConfig, Configuration conf) throws IOException {
        ResourceStore rs = ResourceStore.getStore(kylinConfig);
        String fileKey = cubeSegment.getStatisticsResourcePath();
        InputStream is = rs.getResource(fileKey).inputStream;
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

    // return map of Cuboid ID => MB
    public static Map<Long, Double> getCubeSizeMapFromCuboidStatistics(CubeSegment cubeSegment, KylinConfig kylinConfig, Configuration conf) throws IOException {
        Map<Long, Long> rowCountMap = getCubeRowCountMapFromCuboidStatistics(cubeSegment, kylinConfig, conf);
        Map<Long, Double> sizeMap = getCubeSizeMapFromRowCount(cubeSegment, rowCountMap);
        return sizeMap;
    }

    public static Map<Long, Double> getCubeSizeMapFromRowCount(CubeSegment cubeSegment, Map<Long, Long> rowCountMap) {
        final CubeDesc cubeDesc = cubeSegment.getCubeDesc();
        final List<Integer> rowkeyColumnSize = Lists.newArrayList();
        final long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        final Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        final List<TblColRef> columnList = baseCuboid.getColumns();

        for (int i = 0; i < columnList.size(); i++) {
            rowkeyColumnSize.add(cubeSegment.getColumnLength(columnList.get(i)));
        }

        Map<Long, Double> sizeMap = Maps.newHashMap();
        for (Map.Entry<Long, Long> entry : rowCountMap.entrySet()) {
            sizeMap.put(entry.getKey(), estimateCuboidStorageSize(cubeSegment, entry.getKey(), entry.getValue(), baseCuboidId, rowkeyColumnSize));
        }
        return sizeMap;
    }

    /**
     * Estimate the cuboid's size
     *
     * @return the cuboid size in M bytes
     */
    private static double estimateCuboidStorageSize(CubeSegment cubeSegment, long cuboidId, long rowCount, long baseCuboidId, List<Integer> rowKeyColumnLength) {

        int bytesLength = cubeSegment.getRowKeyPreambleSize();

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
        boolean isMemoryHungry = false;
        for (MeasureDesc measureDesc : cubeSegment.getCubeDesc().getMeasures()) {
            if (measureDesc.getFunction().getMeasureType().isMemoryHungry()) {
                isMemoryHungry = true;
            }
            DataType returnType = measureDesc.getFunction().getReturnDataType();
            space += returnType.getStorageBytesEstimate();
        }
        bytesLength += space;

        double ret = 1.0 * bytesLength * rowCount / (1024L * 1024L);
        if (isMemoryHungry) {
            logger.info("Cube is memory hungry, storage size estimation multiply 0.05");
            ret *= 0.05;
        } else {
            logger.info("Cube is not memory hungry, storage size estimation multiply 0.25");
            ret *= 0.25;
        }
        logger.info("Cuboid " + cuboidId + " has " + rowCount + " rows, each row size is " + bytesLength + " bytes." + " Total size is " + ret + "M.");
        return ret;
    }

    public static void main(String[] args) throws Exception {
        InMemCuboidJob job = new InMemCuboidJob();
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}

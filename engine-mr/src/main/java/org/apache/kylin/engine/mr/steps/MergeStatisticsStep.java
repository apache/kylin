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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CuboidStatsUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.measure.hllc.HyperLogLogPlusCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class MergeStatisticsStep extends AbstractExecutable {
    private static final Logger logger = LoggerFactory.getLogger(MergeStatisticsStep.class);

    protected Map<Long, HyperLogLogPlusCounter> cuboidHLLMap = Maps.newHashMap();

    public MergeStatisticsStep() {
        super();
    }

    @Override
    @SuppressWarnings("deprecation")
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager mgr = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = mgr.getCube(CubingExecutableUtil.getCubeName(this.getParams()));
        final CubeSegment newSegment = cube.getSegmentById(CubingExecutableUtil.getSegmentId(this.getParams()));
        KylinConfig kylinConf = cube.getConfig();

        Configuration conf = HadoopUtil.getCurrentConfiguration();
        ResourceStore rs = ResourceStore.getStore(kylinConf);
        try {

            int averageSamplingPercentage = 0;
            for (String segmentId : CubingExecutableUtil.getMergingSegmentIds(this.getParams())) {
                String fileKey = CubeSegment.getStatisticsResourcePath(CubingExecutableUtil.getCubeName(this.getParams()), segmentId);
                InputStream is = rs.getResource(fileKey).inputStream;
                File tempFile = null;
                FileOutputStream tempFileStream = null;
                try {
                    tempFile = File.createTempFile(segmentId, ".seq");
                    tempFileStream = new FileOutputStream(tempFile);
                    org.apache.commons.io.IOUtils.copy(is, tempFileStream);
                } finally {
                    IOUtils.closeStream(is);
                    IOUtils.closeStream(tempFileStream);
                }

                FileSystem fs = HadoopUtil.getFileSystem("file:///" + tempFile.getAbsolutePath());
                SequenceFile.Reader reader = null;
                try {
                    reader = new SequenceFile.Reader(fs, new Path(tempFile.getAbsolutePath()), conf);
                    LongWritable key = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        if (key.get() == 0L) {
                            // sampling percentage;
                            averageSamplingPercentage += Bytes.toInt(value.getBytes());
                        } else if (key.get() > 0) {
                            HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(kylinConf.getCubeStatsHLLPrecision());
                            ByteArray byteArray = new ByteArray(value.getBytes());
                            hll.readRegisters(byteArray.asBuffer());

                            if (cuboidHLLMap.get(key.get()) != null) {
                                cuboidHLLMap.get(key.get()).merge(hll);
                            } else {
                                cuboidHLLMap.put(key.get(), hll);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                } finally {
                    IOUtils.closeStream(reader);
                    if (tempFile != null)
                        tempFile.delete();
                }
            }
            averageSamplingPercentage = averageSamplingPercentage / CubingExecutableUtil.getMergingSegmentIds(this.getParams()).size();
            CuboidStatsUtil.writeCuboidStatistics(conf, new Path(CubingExecutableUtil.getMergedStatisticsPath(this.getParams())), cuboidHLLMap, averageSamplingPercentage);
            Path statisticsFilePath = new Path(CubingExecutableUtil.getMergedStatisticsPath(this.getParams()), BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);
            FileSystem fs = statisticsFilePath.getFileSystem(conf);
            FSDataInputStream is = fs.open(statisticsFilePath);
            try {
                // put the statistics to metadata store
                String statisticsFileName = newSegment.getStatisticsResourcePath();
                rs.putResource(statisticsFileName, is, System.currentTimeMillis());
            } finally {
                IOUtils.closeStream(is);
            }

            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to merge cuboid statistics", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }

}

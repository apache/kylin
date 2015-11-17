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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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
import org.apache.kylin.common.hll.HyperLogLogPlusCounter;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MergeStatisticsStep extends AbstractExecutable {

    private static final String CUBE_NAME = "cubeName";
    private static final String SEGMENT_ID = "segmentId";
    private static final String MERGING_SEGMENT_IS = "mergingSegmentIds";
    private static final String MERGED_STATISTICS_PATH = "mergedStatisticsPath";
    protected Map<Long, HyperLogLogPlusCounter> cuboidHLLMap = Maps.newHashMap();

    public MergeStatisticsStep() {
        super();
    }

    @Override
    @SuppressWarnings("deprecation")
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig kylinConf = context.getConfig();
        final CubeManager mgr = CubeManager.getInstance(kylinConf);
        final CubeInstance cube = mgr.getCube(getCubeName());
        final CubeSegment newSegment = cube.getSegmentById(getSegmentId());

        Configuration conf = HadoopUtil.getCurrentConfiguration();
        ResourceStore rs = ResourceStore.getStore(kylinConf);
        try {

            int averageSamplingPercentage = 0;
            for (String segmentId : this.getMergingSegmentIds()) {
                String fileKey = CubeSegment.getStatisticsResourcePath(getCubeName(), segmentId);
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
                        if (key.get() == 0l) {
                            // sampling percentage;
                            averageSamplingPercentage += Bytes.toInt(value.getBytes());
                        } else {
                            HyperLogLogPlusCounter hll = new HyperLogLogPlusCounter(14);
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
            averageSamplingPercentage = averageSamplingPercentage / this.getMergingSegmentIds().size();
            CuboidStatsUtil.writeCuboidStatistics(conf, new Path(getMergedStatisticsPath()), cuboidHLLMap, averageSamplingPercentage);
            Path statisticsFilePath = new Path(getMergedStatisticsPath(), BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION);
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

    public void setCubeName(String cubeName) {
        this.setParam(CUBE_NAME, cubeName);
    }

    private String getCubeName() {
        return getParam(CUBE_NAME);
    }

    public void setSegmentId(String segmentId) {
        this.setParam(SEGMENT_ID, segmentId);
    }

    private String getSegmentId() {
        return getParam(SEGMENT_ID);
    }

    public void setMergingSegmentIds(List<String> ids) {
        setParam(MERGING_SEGMENT_IS, StringUtils.join(ids, ","));
    }

    private List<String> getMergingSegmentIds() {
        final String ids = getParam(MERGING_SEGMENT_IS);
        if (ids != null) {
            final String[] splitted = StringUtils.split(ids, ",");
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(splitted.length);
            for (String id : splitted) {
                result.add(id);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    public void setMergedStatisticsPath(String path) {
        setParam(MERGED_STATISTICS_PATH, path);
    }

    private String getMergedStatisticsPath() {
        return getParam(MERGED_STATISTICS_PATH);
    }
}

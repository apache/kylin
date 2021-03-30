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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.CubeStatsWriter;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class MergeDictionaryMapper extends KylinMapper<IntWritable, NullWritable, IntWritable, Text> {
    private static final Logger logger = LoggerFactory.getLogger(MergeDictionaryMapper.class);

    List<CubeSegment> mergingSegments;
    TblColRef[] tblColRefs;
    DictionaryManager dictMgr;

    @Override
    protected void doSetup(Context context) throws IOException, InterruptedException {
        super.doSetup(context);

        final SerializableConfiguration sConf = new SerializableConfiguration(context.getConfiguration());
        final String metaUrl = context.getConfiguration().get(BatchConstants.ARG_META_URL);
        final String cubeName = context.getConfiguration().get(BatchConstants.ARG_CUBE_NAME);
        final String segmentIds = context.getConfiguration().get(MergeDictionaryJob.OPTION_MERGE_SEGMENT_IDS.getOpt());

        final KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);
        final CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        final CubeDesc cubeDesc = CubeDescManager.getInstance(kylinConfig).getCubeDesc(cubeInstance.getDescName());

        mergingSegments = getMergingSegments(cubeInstance, StringUtil.splitByComma(segmentIds));
        tblColRefs = cubeDesc.getAllColumnsNeedDictionaryBuilt().toArray(new TblColRef[0]);
        dictMgr = DictionaryManager.getInstance(kylinConfig);
    }

    @Override
    protected void doMap(IntWritable key, NullWritable value, Context context)
            throws IOException, InterruptedException {

        int index = key.get();

        if (index < tblColRefs.length) {
            // merge dictionary
            TblColRef col = tblColRefs[index];
            List<DictionaryInfo> dictInfos = Lists.newArrayList();
            for (CubeSegment segment : mergingSegments) {
                if (segment.getDictResPath(col) != null) {
                    DictionaryInfo dictInfo = dictMgr.getDictionaryInfo(segment.getDictResPath(col));
                    if (dictInfo != null && !dictInfos.contains(dictInfo)) {
                        dictInfos.add(dictInfo);
                    }
                }
            }

            DictionaryInfo mergedDictInfo = dictMgr.mergeDictionary(dictInfos);
            String tblCol = col.getTableAlias() + ":" + col.getName();
            String dictInfoPath = mergedDictInfo == null ? "" : mergedDictInfo.getResourcePath();

            context.write(new IntWritable(-1), new Text(tblCol + "=" + dictInfoPath));

        } else {
            // merge statistics
            KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(
                    new SerializableConfiguration(context.getConfiguration()),
                    context.getConfiguration().get(BatchConstants.ARG_META_URL));

            final String cubeName = context.getConfiguration().get(BatchConstants.ARG_CUBE_NAME);
            final String segmentId = context.getConfiguration().get(BatchConstants.ARG_SEGMENT_ID);
            final String statOutputPath = context.getConfiguration()
                    .get(MergeDictionaryJob.OPTION_OUTPUT_PATH_STAT.getOpt());
            CubeInstance cubeInstance = CubeManager.getInstance(kylinConfig).getCube(cubeName);

            logger.info("Statistics output path: {}", statOutputPath);

            CubeSegment newSegment = cubeInstance.getSegmentById(segmentId);
            ResourceStore rs = ResourceStore.getStore(kylinConfig);

            Map<Long, HLLCounter> cuboidHLLMap = Maps.newHashMap();
            Configuration conf = null;
            int averageSamplingPercentage = 0;
            long sourceRecordCount = 0;
            long effectiveTimeRange = 0;

            for (CubeSegment cubeSegment : mergingSegments) {
                String filePath = cubeSegment.getStatisticsResourcePath();
                InputStream is = rs.getResource(filePath).content();
                File tempFile;
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
                    conf = HadoopUtil.getCurrentConfiguration();
                    //noinspection deprecation
                    reader = new SequenceFile.Reader(fs, new Path(tempFile.getAbsolutePath()), conf);
                    LongWritable keyW = (LongWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    BytesWritable valueW = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

                    while (reader.next(keyW, valueW)) {
                        if (keyW.get() == 0L) {
                            // sampling percentage;
                            averageSamplingPercentage += Bytes.toInt(valueW.getBytes());
                        } else if (keyW.get() == -3) {
                            long perSourceRecordCount = Bytes.toLong(valueW.getBytes());
                            if (perSourceRecordCount > 0) {
                                sourceRecordCount += perSourceRecordCount;
                                CubeSegment iSegment = cubeInstance.getSegmentById(segmentId);
                                effectiveTimeRange += iSegment.getTSRange().duration();
                            }
                        }  else if (keyW.get() > 0) {
                            HLLCounter hll = new HLLCounter(kylinConfig.getCubeStatsHLLPrecision());
                            ByteArray byteArray = new ByteArray(valueW.getBytes());
                            hll.readRegisters(byteArray.asBuffer());

                            if (cuboidHLLMap.get(keyW.get()) != null) {
                                cuboidHLLMap.get(keyW.get()).merge(hll);
                            } else {
                                cuboidHLLMap.put(keyW.get(), hll);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw e;
                } finally {
                    IOUtils.closeStream(reader);
                }
            }
            sourceRecordCount *= effectiveTimeRange == 0 ? 0
                    : (double) newSegment.getTSRange().duration() / effectiveTimeRange;
            Path statisticsFilePath = new Path(statOutputPath,
                    BatchConstants.CFG_STATISTICS_CUBOID_ESTIMATION_FILENAME);
            averageSamplingPercentage = averageSamplingPercentage / mergingSegments.size();
            CubeStatsWriter.writeCuboidStatistics(conf, new Path(statOutputPath), cuboidHLLMap,
                    averageSamplingPercentage, sourceRecordCount);

            FileSystem fs = HadoopUtil.getFileSystem(statisticsFilePath, conf);
            FSDataInputStream fis = fs.open(statisticsFilePath);

            try {
                // put the statistics to metadata store
                String statisticsFileName = newSegment.getStatisticsResourcePath();
                rs.putResource(statisticsFileName, fis, System.currentTimeMillis());
            } finally {
                IOUtils.closeStream(fis);
            }

            context.write(new IntWritable(-1), new Text(""));
        }
    }

    private List<CubeSegment> getMergingSegments(CubeInstance cube, String[] segmentIds) {
        List<CubeSegment> result = Lists.newArrayListWithCapacity(segmentIds.length);
        for (String id : segmentIds) {
            result.add(cube.getSegmentById(id));
        }
        return result;
    }
}

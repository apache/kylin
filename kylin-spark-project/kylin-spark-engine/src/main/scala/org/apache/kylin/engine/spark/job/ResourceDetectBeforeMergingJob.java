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

package org.apache.kylin.engine.spark.job;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.CubeMergeAssist;
import org.apache.kylin.engine.spark.metadata.MetadataConverter;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import scala.collection.JavaConversions;

public class ResourceDetectBeforeMergingJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeMergingJob.class);

    public static void main(String[] args) {
        ResourceDetectBeforeMergingJob resourceDetectJob = new ResourceDetectBeforeMergingJob();
        resourceDetectJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        logger.info("Start detect resource before merge.");
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);

        final CubeManager cubeManager = CubeManager.getInstance(config);
        final CubeInstance cube = cubeManager.getCubeByUuid(cubeId);
        final CubeSegment mergedSeg = cube.getSegmentById(getParam(MetadataConstants.P_SEGMENT_IDS));
        final SegmentInfo mergedSegInfo = MetadataConverter.getSegmentInfo(cube, mergedSeg.getUuid(),
                mergedSeg.getName(), mergedSeg.getStorageLocationIdentifier());
        final List<CubeSegment> mergingSegments = cube.getMergingSegments(mergedSeg);
        final List<SegmentInfo> segmentInfos = Lists.newArrayList();
        Collections.sort(mergingSegments);
        for (CubeSegment cubeSegment : mergingSegments) {
            segmentInfos.add(MetadataConverter.getSegmentInfo(cube, cubeSegment.getUuid(), cubeSegment.getName(),
                    cubeSegment.getStorageLocationIdentifier()));
        }
        infos.clearMergingSegments();
        infos.recordMergingSegments(segmentInfos);
        Map<Long, CubeMergeAssist> mergeCuboidsAssist = CubeMergeJob.generateMergeAssist(segmentInfos, ss);
        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), ResourceDetectUtils.countDistinctSuffix()),
                ResourceDetectUtils
                        .findCountDistinctMeasure(JavaConversions.asJavaCollection(mergedSegInfo.toBuildLayouts())));
        Map<String, List<String>> resourcePaths = Maps.newHashMap();
        infos.clearSparkPlans();
        for (Map.Entry<Long, CubeMergeAssist> entry : mergeCuboidsAssist.entrySet()) {
            Dataset<Row> afterMerge = entry.getValue().merge(config, getParam(MetadataConstants.P_CUBE_NAME));
            infos.recordSparkPlan(afterMerge.queryExecution().sparkPlan());
            List<Path> paths = JavaConversions
                    .seqAsJavaList(ResourceDetectUtils.getPaths(afterMerge.queryExecution().sparkPlan()));
            List<String> pathStrs = paths.stream().map(Path::toString).collect(Collectors.toList());
            resourcePaths.put(String.valueOf(entry.getKey()), pathStrs);
        }
        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                mergedSeg.getUuid() + "_" + ResourceDetectUtils.fileName()), resourcePaths);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.resourceDetectBeforeMergingJobInfo();
    }

}

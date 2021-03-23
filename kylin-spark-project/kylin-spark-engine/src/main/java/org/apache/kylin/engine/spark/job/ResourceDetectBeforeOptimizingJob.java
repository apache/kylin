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

import org.apache.hadoop.fs.Path;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub;
import org.apache.kylin.engine.spark.metadata.cube.model.ForestSpanningTree;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.engine.spark.utils.SparkUtils;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ResourceDetectBeforeOptimizingJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeOptimizingJob.class);
    protected volatile SpanningTree spanningTree;
    protected volatile List<NBuildSourceInfo> sources = new ArrayList<>();

    public static void main(String[] args) {
        ResourceDetectBeforeOptimizingJob resourceDetectJob = new ResourceDetectBeforeOptimizingJob();
        resourceDetectJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        logger.info("Start detect resource before optimize.");
        String segId = getParam(CubingExecutableUtil.SEGMENT_ID);
        String cubeId = getParam(MetadataConstants.P_CUBE_ID);
        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeManager.getCubeByUuid(cubeId);
        SegmentInfo segInfo = ManagerHub.getSegmentInfo(config, cubeId, segId);
        CubeSegment segment = cubeInstance.getSegmentById(segId);
        infos.recordOptimizingSegment(segInfo);

        spanningTree = new ForestSpanningTree(JavaConversions.asJavaCollection(segInfo.toBuildLayouts()));
        segInfo.removeLayout(segment.getCuboidScheduler().getBaseCuboidId());
        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId), ResourceDetectUtils.countDistinctSuffix()), false);
        ParentSourceChooser datasetChooser = new ParentSourceChooser(spanningTree, segInfo, segment, jobId, ss, config, false);

        datasetChooser.decideSources();
        NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
        if (buildFromFlatTable != null) {
            sources.add(buildFromFlatTable);
        }
        Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();
        sources.addAll(buildFromLayouts.values());

        Map<String, List<String>> resourcePaths = Maps.newHashMap();
        Map<String, Integer> layoutLeafTaskNums = Maps.newHashMap();
        infos.clearSparkPlans();
        for (NBuildSourceInfo source : sources) {
            Dataset<Row> dataset = source.getParentDS();
            RDD actionRdd = dataset.queryExecution().toRdd();
            logger.info("leaf nodes is: {} ", SparkUtils.leafNodes(actionRdd));
            infos.recordSparkPlan(dataset.queryExecution().sparkPlan());
            List<Path> paths = JavaConversions
                    .seqAsJavaList(ResourceDetectUtils.getPaths(dataset.queryExecution().sparkPlan()));
            List<String> pathList = paths.stream().map(Path::toString).collect(Collectors.toList());
            resourcePaths.put(String.valueOf(source.getLayoutId()), pathList);
            layoutLeafTaskNums.put(String.valueOf(source.getLayoutId()), SparkUtils.leafNodePartitionNums(actionRdd));
        }
        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), segId + "_" + ResourceDetectUtils.fileName()),
                resourcePaths);
        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), segId + "_" + ResourceDetectUtils.cubingDetectItemFileSuffix()),
                layoutLeafTaskNums);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.resourceDetectBeforeOptimizeJobInfo();
    }
}

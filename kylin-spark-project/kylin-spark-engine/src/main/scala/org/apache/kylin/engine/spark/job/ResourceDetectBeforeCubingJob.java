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

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.engine.spark.utils.SparkUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.engine.spark.metadata.SegmentInfo;
import org.apache.kylin.engine.spark.metadata.cube.ManagerHub;
import org.apache.kylin.engine.spark.metadata.cube.model.ForestSpanningTree;
import org.apache.kylin.engine.spark.metadata.cube.model.SpanningTree;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

public class ResourceDetectBeforeCubingJob extends SparkApplication {
    protected volatile SpanningTree spanningTree;
    protected volatile List<NBuildSourceInfo> sources = new ArrayList<>();
    protected static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeCubingJob.class);

    @Override
    protected void doExecute() throws Exception {
        logger.info("Start detect resource before cube.");
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(MetadataConstants.P_SEGMENT_IDS)));
        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cubeInstance = cubeManager.getCubeByUuid(getParam(MetadataConstants.P_CUBE_ID));
        for (String segId : segmentIds) {
            SegmentInfo seg = ManagerHub.getSegmentInfo(config, getParam(MetadataConstants.P_CUBE_ID), segId);
            CubeSegment segment = cubeInstance.getSegmentById(segId);
            spanningTree = new ForestSpanningTree(JavaConversions.asJavaCollection(seg.toBuildLayouts()));
            ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId), ResourceDetectUtils.countDistinctSuffix()),
                    ResourceDetectUtils.findCountDistinctMeasure(JavaConversions.asJavaCollection(seg.toBuildLayouts())));
            ParentSourceChooser datasetChooser = new ParentSourceChooser(spanningTree, seg, segment, jobId, ss, config, false);
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
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.resourceDetectBeforeCubingJobInfo();
    }

    public static void main(String[] args) {
        ResourceDetectBeforeCubingJob resourceDetectJob = new ResourceDetectBeforeCubingJob();
        resourceDetectJob.execute(args);
    }

}

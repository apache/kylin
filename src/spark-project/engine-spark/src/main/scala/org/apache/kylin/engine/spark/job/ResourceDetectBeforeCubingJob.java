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

import static org.apache.spark.sql.hive.utils.ResourceDetectUtils.getResourceSize;
import static scala.collection.JavaConverters.asScalaIteratorConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.NBuildSourceInfo;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTree;
import org.apache.kylin.metadata.cube.cuboid.NSpanningTreeFactory;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;
import scala.collection.JavaConversions;

/**
 * @deprecated After KE 4.3, we use {@link RDSegmentBuildJob} to detect build resource
 */
@Deprecated
public class ResourceDetectBeforeCubingJob extends SparkApplication {
    protected volatile NSpanningTree nSpanningTree;
    protected static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeCubingJob.class);

    @Override
    protected void doExecute() throws Exception {
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));

        NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
        IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
        Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream().filter(Objects::nonNull)
                .collect(Collectors.toSet());
        nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId);
        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), ResourceDetectUtils.countDistinctSuffix()),
                ResourceDetectUtils.findCountDistinctMeasure(cuboids));
        for (String segId : segmentIds) {
            List<NBuildSourceInfo> sources = new ArrayList<>();
            NDataSegment seg = dfMgr.getDataflow(dataflowId).getSegment(segId);
            if (Objects.isNull(seg)) {
                logger.info("Skip empty segment {}", segId);
                continue;
            }
            DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, jobId, ss, config, false);
            datasetChooser.decideSources();
            NBuildSourceInfo buildFromFlatTable = datasetChooser.flatTableSource();
            if (buildFromFlatTable != null) {
                sources.add(buildFromFlatTable);
            }
            Map<Long, NBuildSourceInfo> buildFromLayouts = datasetChooser.reuseSources();
            sources.addAll(buildFromLayouts.values());

            Map<String, Long> resourceSize = Maps.newHashMap();
            Map<String, Integer> layoutLeafTaskNums = Maps.newHashMap();
            infos.clearSparkPlans();
            for (NBuildSourceInfo source : sources) {
                Dataset<Row> dataset = source.getParentDS();
                val leafNodeNum = ResourceDetectUtils.getPartitions(dataset.queryExecution().executedPlan());
                logger.info("leaf nodes is: {} ", leafNodeNum);
                infos.recordSparkPlan(dataset.queryExecution().sparkPlan());
                List<Path> paths = JavaConversions
                        .seqAsJavaList(ResourceDetectUtils.getPaths(dataset.queryExecution().sparkPlan(), true));
                resourceSize.put(String.valueOf(source.getLayoutId()),
                        getResourceSize(config, SparderEnv.getHadoopConfiguration(),
                                asScalaIteratorConverter(paths.iterator()).asScala().toSeq()));

                layoutLeafTaskNums.put(String.valueOf(source.getLayoutId()), Integer.parseInt(leafNodeNum));
            }
            ResourceDetectUtils.write(
                    new Path(config.getJobTmpShareDir(project, jobId), segId + "_" + ResourceDetectUtils.fileName()),
                    resourceSize);
            ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                    segId + "_" + ResourceDetectUtils.cubingDetectItemFileSuffix()), layoutLeafTaskNums);
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

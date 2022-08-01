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

import org.apache.hadoop.fs.Path;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.DFLayoutMergeAssist;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

public class ResourceDetectBeforeMergingJob extends SparkApplication implements ResourceDetect {
    protected static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeMergingJob.class);

    @Override
    protected void doExecute() throws Exception {
        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);

        final NDataflowManager mgr = NDataflowManager.getInstance(config, project);
        final NDataflow dataflow = mgr.getDataflow(dataflowId);
        final NDataSegment mergedSeg = dataflow.getSegment(getParam(NBatchConstants.P_SEGMENT_IDS));
        final List<NDataSegment> mergingSegments = dataflow.getMergingSegments(mergedSeg);
        infos.clearMergingSegments();
        Collections.sort(mergingSegments);
        infos.recordMergingSegments(mergingSegments);
        Map<Long, DFLayoutMergeAssist> mergeCuboidsAssist = DFMergeJob.generateMergeAssist(mergingSegments, ss,
                mergedSeg);
        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), ResourceDetectUtils.countDistinctSuffix()),
                ResourceDetectUtils.findCountDistinctMeasure(mergedSeg.getIndexPlan().getAllLayouts()));
        Map<String, Long> resourceSize = Maps.newHashMap();
        infos.clearSparkPlans();
        for (Map.Entry<Long, DFLayoutMergeAssist> entry : mergeCuboidsAssist.entrySet()) {
            Dataset<Row> afterMerge = entry.getValue().merge();
            infos.recordSparkPlan(afterMerge.queryExecution().sparkPlan());
            List<Path> paths = JavaConversions
                    .seqAsJavaList(ResourceDetectUtils.getPaths(afterMerge.queryExecution().sparkPlan()));
            resourceSize.put(String.valueOf(entry.getKey()),
                    ResourceDetectUtils.getResourceSize(SparderEnv.getHadoopConfiguration(),config.isConcurrencyFetchDataSourceSize(),
                            JavaConverters.asScalaIteratorConverter(paths.iterator()).asScala().toSeq()));
        }
        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                mergedSeg.getId() + "_" + ResourceDetectUtils.fileName()), resourceSize);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.resourceDetectBeforeMergingJobInfo();
    }

    public static void main(String[] args) {
        ResourceDetectBeforeMergingJob resourceDetectJob = new ResourceDetectBeforeMergingJob();
        resourceDetectJob.execute(args);
    }

}

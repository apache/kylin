/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.DFLayoutMergeAssist;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import scala.collection.JavaConversions;

public class ResourceDetectBeforeMergingJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeMergingJob.class);

    @Override
    protected void doExecute() throws Exception {
        logger.info("Start detect resource before merge.");
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
        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId), ResourceDetectUtils.countDistinctSuffix()), ResourceDetectUtils.findCountDistinctMeasure(mergedSeg.getIndexPlan().getAllLayouts()));
        Map<String, List<String>> resourcePaths = Maps.newHashMap();
        infos.clearSparkPlans();
        for (Map.Entry<Long, DFLayoutMergeAssist> entry : mergeCuboidsAssist.entrySet()) {
            Dataset<Row> afterMerge = entry.getValue().merge();
            infos.recordSparkPlan(afterMerge.queryExecution().sparkPlan());
            List<Path> paths = JavaConversions
                    .seqAsJavaList(ResourceDetectUtils.getPaths(afterMerge.queryExecution().sparkPlan()));
            List<String> pathStrs = paths.stream().map(Path::toString).collect(Collectors.toList());
            resourcePaths.put(String.valueOf(entry.getKey()), pathStrs);
        }
        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                mergedSeg.getId() + "_" + ResourceDetectUtils.fileName()), resourcePaths);
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

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.engine.spark.utils.SparkUtils;
import io.kyligence.kap.engine.spark.application.SparkApplication;
import io.kyligence.kap.engine.spark.builder.NBuildSourceInfo;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTree;
import io.kyligence.kap.metadata.cube.cuboid.NSpanningTreeFactory;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import scala.collection.JavaConversions;
import lombok.val;

public class ResourceDetectBeforeCubingJob extends SparkApplication {
    protected volatile NSpanningTree nSpanningTree;
    protected volatile List<NBuildSourceInfo> sources = new ArrayList<>();
    protected static final Logger logger = LoggerFactory.getLogger(ResourceDetectBeforeCubingJob.class);

    @Override
    protected void doExecute() throws Exception {
        logger.info("Start detect resource before cube.");

        String dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);
        Set<String> segmentIds = Sets.newHashSet(StringUtils.split(getParam(NBatchConstants.P_SEGMENT_IDS)));
        Set<Long> layoutIds = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS));

        NDataflowManager dfMgr = NDataflowManager.getInstance(config, project);
        IndexPlan indexPlan = dfMgr.getDataflow(dataflowId).getIndexPlan();
        Set<LayoutEntity> cuboids = NSparkCubingUtil.toLayouts(indexPlan, layoutIds).stream().filter(Objects::nonNull)
                .collect(Collectors.toSet());
        nSpanningTree = NSpanningTreeFactory.fromLayouts(cuboids, dataflowId);

        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId), ResourceDetectUtils.countDistinctSuffix()), ResourceDetectUtils.findCountDistinctMeasure(cuboids));
        for (String segId : segmentIds) {
            NDataSegment seg = dfMgr.getDataflow(dataflowId).getSegment(segId);
            DFChooser datasetChooser = new DFChooser(nSpanningTree, seg, jobId, ss, config, false);
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
                val actionRdd = dataset.queryExecution().toRdd();
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

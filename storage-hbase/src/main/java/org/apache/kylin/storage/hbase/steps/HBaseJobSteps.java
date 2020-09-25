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

package org.apache.kylin.storage.hbase.steps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidModeEnum;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.storage.hbase.HBaseConnection;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * Common steps for building cube into HBase
 */
public abstract class HBaseJobSteps extends JobBuilderSupport {

    public HBaseJobSteps(CubeSegment seg) {
        super(seg, null);
    }

    public HadoopShellExecutable createCreateHTableStep(String jobId) {
        return createCreateHTableStep(jobId, CuboidModeEnum.CURRENT);
    }

    // TODO make it abstract
    public HadoopShellExecutable createCreateHTableStep(String jobId, CuboidModeEnum cuboidMode) {
        HadoopShellExecutable createHtableStep = new HadoopShellExecutable();
        createHtableStep.setName(ExecutableConstants.STEP_NAME_CREATE_HBASE_TABLE);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_PARTITION,
                getRowkeyDistributionOutputPath(jobId) + "/part-r-00000");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBOID_MODE, cuboidMode.toString());
        appendExecCmdParameters(cmd, BatchConstants.ARG_HBASE_CONF_PATH, getHBaseConfFilePath(jobId));

        createHtableStep.setJobParams(cmd.toString());
        createHtableStep.setJobClass(CreateHTableJob.class);

        return createHtableStep;
    }

    // TODO make it abstract
    public MapReduceExecutable createMergeCuboidDataStep(CubeSegment seg, List<CubeSegment> mergingSegments,
            String jobID, Class<? extends AbstractHadoopJob> clazz) {
        final List<String> mergingCuboidPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingCuboidPaths.add(getCuboidRootPath(merging) + "*");
        }
        String formattedPath = StringUtil.join(mergingCuboidPaths, ",");
        String outputPath = getCuboidRootPath(jobID);

        MapReduceExecutable mergeCuboidDataStep = new MapReduceExecutable();
        mergeCuboidDataStep.setName(ExecutableConstants.STEP_NAME_MERGE_CUBOID);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, formattedPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME,
                "Kylin_Merge_Cuboid_" + seg.getCubeInstance().getName() + "_Step");

        mergeCuboidDataStep.setMapReduceParams(cmd.toString());
        mergeCuboidDataStep.setMapReduceJobClass(clazz);
        return mergeCuboidDataStep;
    }

    abstract public AbstractExecutable createConvertCuboidToHfileStep(String jobId);

    // TODO make it abstract
    public HadoopShellExecutable createBulkLoadStep(String jobId) {
        HadoopShellExecutable bulkLoadStep = new HadoopShellExecutable();
        bulkLoadStep.setName(ExecutableConstants.STEP_NAME_BULK_LOAD_HFILE);

        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, getHFilePath(jobId));
        appendExecCmdParameters(cmd, BatchConstants.ARG_HTABLE_NAME, seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());

        bulkLoadStep.setJobParams(cmd.toString());
        bulkLoadStep.setJobClass(BulkLoadJob.class);

        return bulkLoadStep;
    }

    public List<CubeSegment> getOptimizeSegments() {
        CubeInstance cube = (CubeInstance) seg.getRealization();
        List<CubeSegment> newSegments = Lists.newArrayList(cube.getSegments(SegmentStatusEnum.READY_PENDING));
        List<CubeSegment> oldSegments = Lists.newArrayListWithExpectedSize(newSegments.size());
        for (CubeSegment segment : newSegments) {
            oldSegments.add(cube.getOriginalSegmentToOptimize(segment));
        }
        return oldSegments;
    }

    public List<String> getOptimizeHTables() {
        return getOldHTables(getOptimizeSegments());
    }

    public List<String> getOldHTables(final List<CubeSegment> oldSegments) {
        final List<String> oldHTables = Lists.newArrayListWithExpectedSize(oldSegments.size());
        for (CubeSegment segment : oldSegments) {
            oldHTables.add(segment.getStorageLocationIdentifier());
        }
        return oldHTables;
    }

    public List<String> getMergingHTables() {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization())
                .getMergingSegments(seg);
        Preconditions.checkState(mergingSegments.size() > 1,
                "there should be more than 2 segments to merge, target segment " + seg);
        return getOldHTables(mergingSegments);
    }

    public List<String> getRefreshingHTables() {
        final CubeSegment refreshingSegment = ((CubeInstance) seg.getRealization()).getOriginalSegmentToRefresh(seg);
        return getOldHTables(Collections.singletonList(refreshingSegment));
    }

    public List<String> getRefreshingHDFSPaths() {
        final CubeSegment refreshingSegment = ((CubeInstance) seg.getRealization()).getOriginalSegmentToRefresh(seg);
        return getOldHDFSPaths(Collections.singletonList(refreshingSegment));
    }

    public List<String> getMergingHDFSPaths() {
        final List<CubeSegment> mergingSegments = ((CubeInstance) seg.getRealization())
                .getMergingSegments(seg);
        Preconditions.checkState(mergingSegments.size() > 1,
                "there should be more than 2 segments to merge, target segment " + seg);
        final List<String> mergingHDFSPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingHDFSPaths.add(getJobWorkingDir(merging.getLastBuildJobID()));
        }
        return mergingHDFSPaths;
    }

    public List<String> getOptimizeHDFSPaths() {
        return getOldHDFSPaths(getOptimizeSegments());
    }

    public List<String> getOldHDFSPaths(final List<CubeSegment> oldSegments) {
        final List<String> oldHDFSPaths = Lists.newArrayListWithExpectedSize(oldSegments.size());
        for (CubeSegment oldSegment : oldSegments) {
            oldHDFSPaths.add(getJobWorkingDir(oldSegment.getLastBuildJobID()));
        }
        return oldHDFSPaths;
    }

    public String getHFilePath(String jobId) {
        return HBaseConnection.makeQualifiedPathInHBaseCluster(
                getJobWorkingDir(jobId) + "/" + seg.getRealization().getName() + "/hfile/");
    }

    public String getRowkeyDistributionOutputPath(String jobId) {
        return HBaseConnection.makeQualifiedPathInHBaseCluster(
                getJobWorkingDir(jobId) + "/" + seg.getRealization().getName() + "/rowkey_stats");
    }

    public void addOptimizeGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        String jobId = jobFlow.getId();

        List<String> toDeletePaths = new ArrayList<>();
        toDeletePaths.add(getOptimizationRootPath(jobId));

        HDFSPathGarbageCollectionStep step =createHDFSPathGCStep(toDeletePaths, jobId);

        jobFlow.addTask(step);
    }

    public void addCheckpointGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        String jobId = jobFlow.getId();

        MergeGCStep hBaseGCStep = createHBaseGCStep(getOptimizeHTables());
        jobFlow.addTask(hBaseGCStep);

        List<String> toDeletePaths = new ArrayList<>();
        toDeletePaths.addAll(getOptimizeHDFSPaths());

        HDFSPathGarbageCollectionStep step = createHDFSPathGCStep(toDeletePaths, jobId);

        jobFlow.addTask(step);
    }

    public void addMergingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        String jobId = jobFlow.getId();

        MergeGCStep hBaseGCStep = createHBaseGCStep(getMergingHTables());
        jobFlow.addTask(hBaseGCStep);

        List<String> toDeletePaths = new ArrayList<>();
        toDeletePaths.addAll(getMergingHDFSPaths());
        toDeletePaths.add(getHFilePath(jobId));

        HDFSPathGarbageCollectionStep step = createHDFSPathGCStep(toDeletePaths, jobId);

        jobFlow.addTask(step);
    }

    public void addCubingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        String jobId = jobFlow.getId();

        List<String> toDeletePaths = new ArrayList<>();
        toDeletePaths.add(getFactDistinctColumnsPath(jobId));
        toDeletePaths.add(getHFilePath(jobId));
        toDeletePaths.add(getShrunkenDictionaryPath(jobId));

        CubeSegment oldSegment = ((CubeInstance)seg.getRealization()).getOriginalSegmentToRefresh(seg);

        // refresh segment
        if (oldSegment != null && KylinConfig.getInstanceFromEnv().cleanStorageAfterDelOperation()) {
            // delete old hdfs job
            toDeletePaths.addAll(getRefreshingHDFSPaths());

            // drop old htables
            MergeGCStep hBaseGCStep = createHBaseGCStep(getRefreshingHTables());
            jobFlow.addTask(hBaseGCStep);
        }

        HDFSPathGarbageCollectionStep step = createHDFSPathGCStep(toDeletePaths, jobId);
        jobFlow.addTask(step);
    }

    /**
     * create 'HBase Garbage clean step' to drop HTables in HBase
     * @param toDropHTables
     * @return
     */
    public MergeGCStep createHBaseGCStep(List<String> toDropHTables) {
        MergeGCStep hBaseGCStep = new MergeGCStep();
        hBaseGCStep.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION_HBASE);
        hBaseGCStep.setOldHTables(toDropHTables);
        return hBaseGCStep;
    }

    /**
     * create 'HDFS Garbage clean step' to delete paths on HDFS
     * @param toDeletePaths
     * @param jobId
     * @return
     */
    public HDFSPathGarbageCollectionStep createHDFSPathGCStep(List<String> toDeletePaths, String jobId) {
        HDFSPathGarbageCollectionStep hdfsGCStep = new HDFSPathGarbageCollectionStep();
        hdfsGCStep.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION_HDFS);
        hdfsGCStep.setDeletePaths(toDeletePaths);
        hdfsGCStep.setJobId(jobId);
        return hdfsGCStep;
    }

}

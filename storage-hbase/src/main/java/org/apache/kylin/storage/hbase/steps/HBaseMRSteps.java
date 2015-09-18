package org.apache.kylin.storage.hbase.steps;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.common.HadoopShellExecutable;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.RangeKeyDistributionJob;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class HBaseMRSteps extends JobBuilderSupport {

    public HBaseMRSteps(CubeSegment seg) {
        super(seg, null);
    }

    public void addSaveCuboidToHTableSteps(DefaultChainedExecutable jobFlow, String cuboidRootPath) {
        String jobId = jobFlow.getId();

        // calculate key distribution
        jobFlow.addTask(createRangeRowkeyDistributionStep(cuboidRootPath, jobId));
        // create htable step
        jobFlow.addTask(createCreateHTableStep(jobId));
        // generate hfiles step
        jobFlow.addTask(createConvertCuboidToHfileStep(cuboidRootPath, jobId));
        // bulk load step
        jobFlow.addTask(createBulkLoadStep(jobId));
    }

    public MapReduceExecutable createRangeRowkeyDistributionStep(String cuboidRootPath, String jobId) {
        String inputPath = cuboidRootPath + (cuboidRootPath.endsWith("/") ? "" : "/") + "*";
        
        MapReduceExecutable rowkeyDistributionStep = new MapReduceExecutable();
        rowkeyDistributionStep.setName(ExecutableConstants.STEP_NAME_GET_CUBOID_KEY_DISTRIBUTION);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getRowkeyDistributionOutputPath(jobId));
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "jobname", "Kylin_Region_Splits_Calculator_" + seg.getCubeInstance().getName() + "_Step");

        rowkeyDistributionStep.setMapReduceParams(cmd.toString());
        rowkeyDistributionStep.setMapReduceJobClass(RangeKeyDistributionJob.class);
        return rowkeyDistributionStep;
    }

    public HadoopShellExecutable createCreateHTableStep(String jobId) {
        return createCreateHTableStep(jobId, false);
    }

    public HadoopShellExecutable createCreateHTableStepWithStats(String jobId) {
        return createCreateHTableStep(jobId, true);
    }

    private HadoopShellExecutable createCreateHTableStep(String jobId, boolean withStats) {
        HadoopShellExecutable createHtableStep = new HadoopShellExecutable();
        createHtableStep.setName(ExecutableConstants.STEP_NAME_CREATE_HBASE_TABLE);
        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "segmentname", seg.getName());
        appendExecCmdParameters(cmd, "input", getRowkeyDistributionOutputPath(jobId) + "/part-r-00000");
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "statisticsenabled", String.valueOf(withStats));

        createHtableStep.setJobParams(cmd.toString());
        createHtableStep.setJobClass(CreateHTableJob.class);

        return createHtableStep;
    }

    public MapReduceExecutable createConvertCuboidToHfileStep(String cuboidRootPath, String jobId) {
        String inputPath = cuboidRootPath + (cuboidRootPath.endsWith("/") ? "" : "/") + "*";
        
        MapReduceExecutable createHFilesStep = new MapReduceExecutable();
        createHFilesStep.setName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd, seg);
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());
        appendExecCmdParameters(cmd, "input", inputPath);
        appendExecCmdParameters(cmd, "output", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "jobname", "Kylin_HFile_Generator_" + seg.getCubeInstance().getName() + "_Step");

        createHFilesStep.setMapReduceParams(cmd.toString());
        createHFilesStep.setMapReduceJobClass(CubeHFileJob.class);
        createHFilesStep.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES);

        return createHFilesStep;
    }

    public HadoopShellExecutable createBulkLoadStep(String jobId) {
        HadoopShellExecutable bulkLoadStep = new HadoopShellExecutable();
        bulkLoadStep.setName(ExecutableConstants.STEP_NAME_BULK_LOAD_HFILE);

        StringBuilder cmd = new StringBuilder();
        appendExecCmdParameters(cmd, "input", getHFilePath(jobId));
        appendExecCmdParameters(cmd, "htablename", seg.getStorageLocationIdentifier());
        appendExecCmdParameters(cmd, "cubename", seg.getCubeInstance().getName());

        bulkLoadStep.setJobParams(cmd.toString());
        bulkLoadStep.setJobClass(BulkLoadJob.class);

        return bulkLoadStep;
    }

    public MergeGCStep createMergeGCStep() {
        MergeGCStep result = new MergeGCStep();
        result.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        result.setOldHTables(getMergingHTables());
        return result;
    }

    public List<String> getMergingHTables() {
        final List<CubeSegment> mergingSegments = seg.getCubeInstance().getMergingSegments(seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingHTables = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingHTables.add(merging.getStorageLocationIdentifier());
        }
        return mergingHTables;
    }

    public List<String> getMergingHDFSPaths() {
        final List<CubeSegment> mergingSegments = seg.getCubeInstance().getMergingSegments(seg);
        Preconditions.checkState(mergingSegments.size() > 1, "there should be more than 2 segments to merge");
        final List<String> mergingHDFSPaths = Lists.newArrayList();
        for (CubeSegment merging : mergingSegments) {
            mergingHDFSPaths.add(getJobWorkingDir(merging.getLastBuildJobID()));
        }
        return mergingHDFSPaths;
    }

    public String getHFilePath(String jobId) {
        return HadoopUtil.makeQualifiedPathInHBaseCluster(getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/hfile/");
    }

    public String getRowkeyDistributionOutputPath(String jobId) {
        return HadoopUtil.makeQualifiedPathInHBaseCluster(getJobWorkingDir(jobId) + "/" + seg.getCubeInstance().getName() + "/rowkey_stats");
    }

    public void addMergingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        String jobId = jobFlow.getId();

        jobFlow.addTask(createMergeGCStep());

        List<String> toDeletePathsOnHadoopCluster = new ArrayList<>();
        toDeletePathsOnHadoopCluster.addAll(getMergingHDFSPaths());

        List<String> toDeletePathsOnHbaseCluster = new ArrayList<>();
        toDeletePathsOnHbaseCluster.add(getRowkeyDistributionOutputPath(jobId));
        toDeletePathsOnHbaseCluster.add(getHFilePath(jobId));

        HDFSPathGarbageCollectionStep step = new HDFSPathGarbageCollectionStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setDeletePathsOnHadoopCluster(toDeletePathsOnHadoopCluster);
        step.setDeletePathsOnHBaseCluster(toDeletePathsOnHbaseCluster);
        step.setJobId(jobId);

        jobFlow.addTask(step);
    }

    public void addCubingGarbageCollectionSteps(DefaultChainedExecutable jobFlow) {
        String jobId = jobFlow.getId();

        List<String> toDeletePathsOnHadoopCluster = new ArrayList<>();
        toDeletePathsOnHadoopCluster.add(getFactDistinctColumnsPath(jobId));

        List<String> toDeletePathsOnHbaseCluster = new ArrayList<>();
        toDeletePathsOnHbaseCluster.add(getRowkeyDistributionOutputPath(jobId));
        toDeletePathsOnHbaseCluster.add(getHFilePath(jobId));

        HDFSPathGarbageCollectionStep step = new HDFSPathGarbageCollectionStep();
        step.setName(ExecutableConstants.STEP_NAME_GARBAGE_COLLECTION);
        step.setDeletePathsOnHadoopCluster(toDeletePathsOnHadoopCluster);
        step.setDeletePathsOnHBaseCluster(toDeletePathsOnHbaseCluster);
        step.setJobId(jobId);

        jobFlow.addTask(step);
    }
}

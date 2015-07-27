package org.apache.kylin.engine.spark;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.mr.*;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.spark.SparkCubing;
import org.apache.kylin.job.spark.SparkExecutable;

/**
 */
public class SparkCubingJobBuilder extends JobBuilderSupport {

    private final IMRInput.IMRBatchCubingInputSide inputSide;
    private final IMROutput2.IMRBatchCubingOutputSide2 outputSide;
    
    public SparkCubingJobBuilder(CubeSegment seg, String submitter) {
        super(seg, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        this.outputSide = MRUtil.getBatchCubingOutputSide2(seg);
    }

    public DefaultChainedExecutable build() {
        final CubingJob result = CubingJob.createBuildJob(seg, submitter, config);
        final String jobId = result.getId();
        
//        inputSide.addStepPhase1_CreateFlatTable(result);
//        final CubeJoinedFlatTableDesc joinedFlatTableDesc = new CubeJoinedFlatTableDesc(seg.getCubeDesc(), seg);
//        final String tableName = joinedFlatTableDesc.getTableName();
//        logger.info("intermediate table:" + tableName);

        final String tableName = "kylin_intermediate_test_kylin_cube_with_slr_left_join_desc_19700101000000_20501112000000"; 
        final SparkExecutable sparkExecutable = new SparkExecutable();
        sparkExecutable.setClassName(SparkCubing.class.getName());
        sparkExecutable.setParam("hiveTable", tableName);
        result.addTask(sparkExecutable);
        return result;
    }
}

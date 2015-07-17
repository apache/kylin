package org.apache.kylin.engine.mr;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

public interface IMROutput2 {

    /** Return a helper to participate in batch cubing job flow. */
    public IMRBatchCubingOutputSide2 getBatchCubingOutputSide(CubeSegment seg);

    /**
     * Participate the batch cubing flow as the output side.
     * 
     * - Phase 1: Create Flat Table
     * - Phase 2: Build Dictionary
     * - Phase 3: Build Cube (with StorageOutputFormat)
     * - Phase 4: Update Metadata & Cleanup
     */
    public interface IMRBatchCubingOutputSide2 {

        public IMRStorageOutputFormat getStorageOutputFormat();

        /** Add step that executes after build dictionary and before build cube. */
        public void addStepPhase2_BuildDictionary(DefaultChainedExecutable jobFlow);

        /** Add step that executes after build cube. */
        public void addStepPhase3_BuildCube(DefaultChainedExecutable jobFlow);

        /** Add step that does any necessary clean up. */
        public void addStepPhase4_Cleanup(DefaultChainedExecutable jobFlow);
    }

    public IMRBatchMergeInputSide2 getBatchMergeInputSide(CubeSegment seg);

    public interface IMRBatchMergeInputSide2 {
        public IMRStorageInputFormat getStorageInputFormat();
    }

    @SuppressWarnings("rawtypes")
    public interface IMRStorageInputFormat {
        
        public void configureInput(Class<? extends Mapper> mapper, Class<? extends WritableComparable> outputKeyClass, Class<? extends Writable> outputValueClass, Job job) throws IOException;
        
        public CubeSegment findSourceSegment(Mapper.Context context, CubeInstance cubeInstance) throws IOException;
        
        public Pair<ByteArrayWritable, Object[]> parseMapperInput(Object inKey, Object inValue);
    }

    /** Return a helper to participate in batch merge job flow. */
    public IMRBatchMergeOutputSide2 getBatchMergeOutputSide(CubeSegment seg);

    /**
     * Participate the batch merge flow as the output side.
     * 
     * - Phase 1: Merge Dictionary
     * - Phase 2: Merge Cube (with StorageInputFormat & StorageOutputFormat)
     * - Phase 3: Update Metadata & Cleanup
     */
    public interface IMRBatchMergeOutputSide2 {

        public IMRStorageOutputFormat getStorageOutputFormat();

        /** Add step that executes after merge dictionary and before merge cube. */
        public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow);

        /** Add step that executes after merge cube. */
        public void addStepPhase2_BuildCube(DefaultChainedExecutable jobFlow);

        /** Add step that does any necessary clean up. */
        public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow);
    }

    @SuppressWarnings("rawtypes")
    public interface IMRStorageOutputFormat {
        public void configureOutput(Class<? extends Reducer> reducer, String jobFlowId, Job job) throws IOException;
        
        public void doReducerOutput(ByteArrayWritable key, Object[] value, Reducer.Context context) throws IOException, InterruptedException;
    }
}

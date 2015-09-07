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

        /** Return an output format for Phase 3: Build Cube MR */
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

    /** Read in a cube as input of merge. Configure the input file format of mapper. */
    @SuppressWarnings("rawtypes")
    public interface IMRStorageInputFormat {

        /** Configure MR mapper class and input file format. */
        public void configureInput(Class<? extends Mapper> mapper, Class<? extends WritableComparable> outputKeyClass, Class<? extends Writable> outputValueClass, Job job) throws IOException;

        /** Given a mapper context, figure out which segment the mapper reads from. */
        public CubeSegment findSourceSegment(Mapper.Context context, CubeInstance cubeInstance) throws IOException;

        /**
         * Read in a row of cuboid. Given the input KV, de-serialize back cuboid ID, dimensions, and measures.
         * 
         * @return <code>ByteArrayWritable</code> is the cuboid ID (8 bytes) + dimension values in dictionary encoding
         *         <code>Object[]</code> is the measure values in order of <code>CubeDesc.getMeasures()</code>
         */
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

        /** Return an input format for Phase 2: Merge Cube MR */
        public IMRStorageOutputFormat getStorageOutputFormat();

        /** Add step that executes after merge dictionary and before merge cube. */
        public void addStepPhase1_MergeDictionary(DefaultChainedExecutable jobFlow);

        /** Add step that executes after merge cube. */
        public void addStepPhase2_BuildCube(DefaultChainedExecutable jobFlow);

        /** Add step that does any necessary clean up. */
        public void addStepPhase3_Cleanup(DefaultChainedExecutable jobFlow);
    }

    /** Write out a cube. Configure the output file format of reducer and do the actual K-V output. */
    @SuppressWarnings("rawtypes")
    public interface IMRStorageOutputFormat {
        
        /** Configure MR reducer class and output file format. */
        public void configureOutput(Class<? extends Reducer> reducer, String jobFlowId, Job job) throws IOException;

        /**
         * Write out a row of cuboid. Given the cuboid ID, dimensions, and measures, serialize in whatever
         * way and output to reducer context.
         * 
         * @param key     The cuboid ID (8 bytes) + dimension values in dictionary encoding
         * @param value   The measure values in order of <code>CubeDesc.getMeasures()</code>
         * @param context The reducer context output goes to
         */
        public void doReducerOutput(ByteArrayWritable key, Object[] value, Reducer.Context context) throws IOException, InterruptedException;
    }
}

package org.apache.kylin.engine.spark;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.IBatchCubingEngine;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

/**
 */
public class SparkBatchCubingEngine implements IBatchCubingEngine {
    @Override
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter) {
        return new SparkCubingJobBuilder(newSegment , submitter).build();
    }

    @Override
    public DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter) {
        return null;
    }

    @Override
    public Class<?> getSourceInterface() {
        return null;
    }

    @Override
    public Class<?> getStorageInterface() {
        return null;
    }
}

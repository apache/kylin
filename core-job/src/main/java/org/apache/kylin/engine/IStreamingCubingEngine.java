package org.apache.kylin.engine;

import org.apache.kylin.cube.CubeSegment;

public interface IStreamingCubingEngine {

    public Runnable createStreamingCubingBuilder(CubeSegment seg);
}

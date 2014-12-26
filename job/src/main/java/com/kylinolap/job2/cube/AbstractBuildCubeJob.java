package com.kylinolap.job2.cube;

import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;

/**
 * Created by qianzhou on 12/25/14.
 */
public abstract class AbstractBuildCubeJob extends AbstractExecutable {

    private static final String CUBE_INSTANCE_NAME = "cubeName";
    private static final String CUBE_SEGMENT_NAME = "segmentName";

    public AbstractBuildCubeJob() {
    }

    public AbstractBuildCubeJob(JobPO job, JobOutputPO jobOutput) {
        super(job, jobOutput);
    }

    void setCubeInstanceName(String name) {
        setParam(CUBE_INSTANCE_NAME, name);
    }

    public String getCubeInstanceName() {
        return getParam(CUBE_INSTANCE_NAME);
    }

    void setCubeSegmentName(String name) {
        setParam(CUBE_SEGMENT_NAME, name);
    }

    public String getCubeSegmentName() {
        return getParam(CUBE_SEGMENT_NAME);
    }
}

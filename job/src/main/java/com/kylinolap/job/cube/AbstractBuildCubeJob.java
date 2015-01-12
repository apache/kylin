package com.kylinolap.job.cube;

import com.kylinolap.job.dao.JobOutputPO;
import com.kylinolap.job.dao.JobPO;
import com.kylinolap.job.impl.threadpool.AbstractExecutable;

/**
 * Created by qianzhou on 12/25/14.
 */
public abstract class AbstractBuildCubeJob extends AbstractExecutable {

    private static final String CUBE_INSTANCE_NAME = "cubeName";
    private static final String CUBE_SEGMENT_NAME = "segmentName";

    public AbstractBuildCubeJob() {
    }

    public AbstractBuildCubeJob(JobPO job) {
        super(job);
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

package com.kylinolap.job2.cube;

import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.impl.threadpool.DefaultChainedExecutable;

/**
 * Created by qianzhou on 12/25/14.
 */
public class BuildCubeJob extends DefaultChainedExecutable {

    public BuildCubeJob() {
        super();
    }

    public BuildCubeJob(JobPO job) {
        super(job);
    }

    public static final String CUBE_INSTANCE_NAME = "cubeName";
    public static final String SEGMENT_ID = "segmentId";


    void setCubeName(String name) {
        setParam(CUBE_INSTANCE_NAME, name);
    }

    public String getCubeName() {
        return getParam(CUBE_INSTANCE_NAME);
    }

    public void setSegmentId(String segmentId) {
        setParam(SEGMENT_ID, segmentId);
    }

    public String getSegmentId() {
        return getParam(SEGMENT_ID);
    }

}

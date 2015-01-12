package com.kylinolap.job;

import com.kylinolap.job.dao.JobPO;
import com.kylinolap.job.impl.threadpool.AbstractExecutable;

/**
 * Created by qianzhou on 12/16/14.
 */
public abstract class BaseTestExecutable extends AbstractExecutable {


    public BaseTestExecutable() {
    }

    public BaseTestExecutable(JobPO job) {
        super(job);
    }

}

package com.kylinolap.job2;

import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;

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

package com.kylinolap.job;

import com.kylinolap.job.dao.ExecutablePO;
import com.kylinolap.job.execution.AbstractExecutable;

/**
 * Created by qianzhou on 12/16/14.
 */
public abstract class BaseTestExecutable extends AbstractExecutable {


    public BaseTestExecutable() {
    }

    public BaseTestExecutable(ExecutablePO job) {
        super(job);
    }

}

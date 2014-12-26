package com.kylinolap.job2.common;

import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecuteResult;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;

/**
 * Created by qianzhou on 12/25/14.
 */
public class MapReduceExecutable extends AbstractExecutable {

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        return null;
    }

    @Override
    public boolean isRunnable() {
        return false;
    }
}

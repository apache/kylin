package com.kylinolap.job2;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.execution.ExecutableContext;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.execution.ExecuteResult;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.service.DefaultJobService;

import java.util.UUID;

/**
 * Created by qianzhou on 12/16/14.
 */
public abstract class BaseTestExecutable extends AbstractExecutable {


    public BaseTestExecutable() {
    }

    public BaseTestExecutable(JobPO job, JobOutputPO jobOutput) {
        super(job, jobOutput);
    }

    @Override
    public boolean isRunnable() {
        return getStatus() == ExecutableStatus.READY;
    }
}

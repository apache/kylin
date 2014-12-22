package com.kylinolap.job2;

import com.kylinolap.common.KylinConfig;
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

    private static DefaultJobService jobService = DefaultJobService.getInstance(KylinConfig.getInstanceFromEnv());


    @Override
    protected void onExecuteStart(ExecutableContext executableContext) {
        this.setStatus(ExecutableStatus.RUNNING);
        jobService.updateJobStatus(this);
    }

    @Override
    protected void onExecuteSucceed(ExecuteResult result, ExecutableContext executableContext) {
        if (result.succeed()) {
            this.setStatus(ExecutableStatus.SUCCEED);
        } else {
            this.setStatus(ExecutableStatus.ERROR);
        }
        this.setOutput(result.output());
        jobService.updateJobStatus(this);
    }

    @Override
    protected void onExecuteError(Throwable exception, ExecutableContext executableContext) {
        this.setStatus(ExecutableStatus.ERROR);
        this.setOutput(exception.getLocalizedMessage());
        jobService.updateJobStatus(this);
    }

    @Override
    public boolean isRunnable() {
        return getStatus() == ExecutableStatus.READY;
    }
}

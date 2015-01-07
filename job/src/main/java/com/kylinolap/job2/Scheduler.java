package com.kylinolap.job2;

import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.exception.SchedulerException;
import com.kylinolap.job2.execution.Executable;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Scheduler<T extends Executable> {

    void init(JobEngineConfig jobEngineConfig) throws SchedulerException;

    void shutdown() throws SchedulerException;

    boolean stop(T executable) throws SchedulerException;

}

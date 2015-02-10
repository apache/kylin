package com.kylinolap.job;

import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job.exception.SchedulerException;
import com.kylinolap.job.execution.Executable;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Scheduler<T extends Executable> {

    void init(JobEngineConfig jobEngineConfig) throws SchedulerException;

    void shutdown() throws SchedulerException;

    boolean stop(T executable) throws SchedulerException;

}

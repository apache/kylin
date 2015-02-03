package org.apache.kylin.job;

import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.Executable;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Scheduler<T extends Executable> {

    void init(JobEngineConfig jobEngineConfig) throws SchedulerException;

    void shutdown() throws SchedulerException;

    boolean stop(T executable) throws SchedulerException;

}

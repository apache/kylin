package com.kylinolap.job2;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.engine.JobEngineConfig;
import com.kylinolap.job2.exception.SchedularException;
import com.kylinolap.job2.execution.Executable;

import java.util.List;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Scheduler {

    void init(JobEngineConfig jobEngineConfig) throws SchedularException;

    void shutdown() throws SchedularException;

    boolean submit(Executable executable) throws SchedularException;

    boolean stop(Executable executable) throws SchedularException;

}

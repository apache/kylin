package com.kylinolap.job2.schedular;

import com.kylinolap.job2.exception.SchedularException;
import com.kylinolap.job2.execution.Executable;

import java.util.List;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface Scheduler {

    void submit(Executable executable) throws SchedularException;

    void stop(Executable executable) throws SchedularException;

    List<Executable> getAllExecutables();

}

package com.kylinolap.job2.schedular;

import com.kylinolap.job2.exception.SchedularException;
import com.kylinolap.job2.execution.Executable;

import java.util.List;

/**
 * Created by qianzhou on 12/15/14.
 */
public class DefaultSchedular implements Scheduler {
    @Override
    public void submit(Executable executable) throws SchedularException {

    }

    @Override
    public void stop(Executable executable) throws SchedularException {

    }

    @Override
    public List<Executable> getAllExecutables() {
        return null;
    }
}

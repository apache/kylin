package com.kylinolap.job2.impl.threadpool;

import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.execution.ExecutableContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by qianzhou on 12/16/14.
 */
public class DefaultContext implements ExecutableContext {

    private final ConcurrentMap<String, Executable> runningJobs;

    public DefaultContext(ConcurrentMap<String, Executable> runningJobs) {
        this.runningJobs = runningJobs;
    }
    @Override
    public Object getSchedulerContext() {
        return null;
    }

    public void addRunningJob(Executable executable) {
        runningJobs.put(executable.getId(), executable);
    }

    public void removeRunningJob(Executable executable) {
        runningJobs.remove(executable.getId());
    }

    public Map<String, Executable> getRunningJobs() {
        return Collections.unmodifiableMap(runningJobs);
    }
}

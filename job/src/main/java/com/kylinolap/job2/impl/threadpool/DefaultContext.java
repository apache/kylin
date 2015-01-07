package com.kylinolap.job2.impl.threadpool;

import com.kylinolap.common.KylinConfig;
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
    private final KylinConfig kylinConfig;

    public DefaultContext(ConcurrentMap<String, Executable> runningJobs, KylinConfig kylinConfig) {
        this.runningJobs = runningJobs;
        this.kylinConfig = kylinConfig;
    }
    @Override
    public Object getSchedulerContext() {
        return null;
    }

    @Override
    public KylinConfig getConfig() {
        return kylinConfig;
    }

    void addRunningJob(Executable executable) {
        runningJobs.put(executable.getId(), executable);
    }

    void removeRunningJob(Executable executable) {
        runningJobs.remove(executable.getId());
    }

    public Map<String, Executable> getRunningJobs() {
        return Collections.unmodifiableMap(runningJobs);
    }
}

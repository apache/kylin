package com.kylinolap.job2.impl.quartz;

import com.google.common.base.Preconditions;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.execution.ExecutableContext;
import org.quartz.JobExecutionContext;

/**
 * Created by qianzhou on 12/15/14.
 */
public class QuartzContext implements ExecutableContext {

    private final KylinConfig kylinConfig;
    private JobExecutionContext innerContext;

    public QuartzContext(JobExecutionContext context, KylinConfig kylinConfig) {
        Preconditions.checkArgument(context != null, "context cannot be null");
        innerContext = context;
        this.kylinConfig = kylinConfig;
    }
    @Override
    public JobExecutionContext getSchedulerContext() {
        return innerContext;
    }

    @Override
    public KylinConfig getConfig() {
        return kylinConfig;
    }
}

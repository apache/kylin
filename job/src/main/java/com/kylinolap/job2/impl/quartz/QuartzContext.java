package com.kylinolap.job2.impl.quartz;

import com.google.common.base.Preconditions;
import com.kylinolap.job2.execution.ExecutableContext;
import org.quartz.JobExecutionContext;

/**
 * Created by qianzhou on 12/15/14.
 */
public class QuartzContext implements ExecutableContext {

    private JobExecutionContext innerContext;

    public QuartzContext(JobExecutionContext context) {
        Preconditions.checkArgument(context != null, "context cannot be null");
        innerContext = context;
    }
    @Override
    public JobExecutionContext getSchedulerContext() {
        return innerContext;
    }
}

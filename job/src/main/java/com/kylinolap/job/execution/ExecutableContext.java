package com.kylinolap.job.execution;

import com.kylinolap.common.KylinConfig;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface ExecutableContext {

    Object getSchedulerContext();

    KylinConfig getConfig();
}

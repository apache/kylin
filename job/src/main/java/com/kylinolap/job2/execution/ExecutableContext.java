package com.kylinolap.job2.execution;

import com.kylinolap.common.KylinConfig;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface ExecutableContext {

    Object getSchedulerContext();

    KylinConfig getConfig();
}

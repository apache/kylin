package com.kylinolap.job.execution;

import org.apache.kylin.common.KylinConfig;

/**
 * Created by qianzhou on 12/15/14.
 */
public interface ExecutableContext {

    Object getSchedulerContext();

    KylinConfig getConfig();
}

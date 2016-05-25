package org.apache.kylin.job;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ImplementationSwitch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 */
public class SchedulerFactory {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerFactory.class);
    private static ImplementationSwitch<Scheduler> schedulers;

    static {
        Map<Integer, String> impls = KylinConfig.getInstanceFromEnv().getSchedulers();
        schedulers = new ImplementationSwitch<Scheduler>(impls, Scheduler.class);
    }

    public static Scheduler scheduler(int schedulerType) {
        return schedulers.get(schedulerType);
    }

}

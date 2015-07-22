package org.apache.kylin.common.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 */
public class DaemonThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable r) {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setDaemon(true);
        return t;
    }
}

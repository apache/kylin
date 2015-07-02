package org.apache.kylin.common.lock;

/**
 */
public interface JobLock {
    boolean lock();

    void unlock();
}

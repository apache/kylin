package org.apache.kylin.job.lock;

/**
 */
public interface JobLock {
    boolean lock();

    void unlock();
}

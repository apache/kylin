package org.apache.kylin.common.lock;

/**
 * Created by Hongbin Ma(Binmahone) on 5/26/15.
 */
public interface JobLock {
    boolean lock();

    void unlock();
}

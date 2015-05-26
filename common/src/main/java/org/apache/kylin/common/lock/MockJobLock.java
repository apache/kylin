package org.apache.kylin.common.lock;

/**
 * Created by Hongbin Ma(Binmahone) on 5/26/15.
 */
public class MockJobLock implements JobLock {
    @Override
    public boolean lock() {
        return true;
    }

    @Override
    public void unlock() {
        return;
    }
}

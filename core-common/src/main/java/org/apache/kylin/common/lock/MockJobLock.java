package org.apache.kylin.common.lock;

/**
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

package com.kylinolap.job2.impl.threadpool;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.job.engine.JobEngineConfig;
import org.junit.After;
import org.junit.Before;

/**
 * Created by qianzhou on 12/19/14.
 */
public class DefaultSchedulerTest extends LocalFileMetadataTestCase {

    private DefaultScheduler scheduler;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        scheduler = DefaultScheduler.getInstance();
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }
}

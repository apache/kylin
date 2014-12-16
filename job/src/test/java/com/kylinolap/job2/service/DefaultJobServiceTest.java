package com.kylinolap.job2.service;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.job2.TestExecutable;
import com.kylinolap.job2.execution.ExecuteStatus;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.sun.org.apache.bcel.internal.generic.NEW;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by qianzhou on 12/16/14.
 */
public class DefaultJobServiceTest extends LocalFileMetadataTestCase {

    private DefaultJobService service;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        service = DefaultJobService.getInstance(KylinConfig.getInstanceFromEnv());

        for (AbstractExecutable executable: service.getAllExecutables()) {
            System.out.println("deleting " + executable.getId());
            service.delete(executable);
        }

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void test() throws Exception {
        assertNotNull(service);
        TestExecutable executable = new TestExecutable();
        executable.setAsync(true);
        executable.setStatus(ExecuteStatus.NEW);
        service.add(executable);
        List<AbstractExecutable> result = service.getAllExecutables();
        assertEquals(1, result.size());
        AbstractExecutable another = service.get(executable.getId());
        assertEquals(executable.getId(), another.getId());
        assertEquals(executable.getStatus(), another.getStatus());
        assertEquals(executable.isRunnable(), another.isRunnable());
        assertEquals(executable.isAsync(), another.isAsync());
    }
}

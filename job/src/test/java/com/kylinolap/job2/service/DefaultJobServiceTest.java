package com.kylinolap.job2.service;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.job2.TestExecutable;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

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
            service.deleteJob(executable);
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
        executable.setStatus(ExecutableStatus.READY);
        HashMap<String, String> extra = new HashMap<>();
        extra.put("test1", "test1");
        extra.put("test2", "test2");
        extra.put("test3", "test3");
        executable.setExtra(extra);
        service.addJob(executable);
        List<AbstractExecutable> result = service.getAllExecutables();
        assertEquals(1, result.size());
        AbstractExecutable another = service.getJob(executable.getId());
        assertEqual(executable, another);

        executable.setStatus(ExecutableStatus.SUCCEED);
        executable.setOutput("test output");
        service.updateJobStatus(executable);
        assertEqual(executable, service.getJob(executable.getId()));
    }

    private void assertEqual(AbstractExecutable one, AbstractExecutable another) {
        assertEquals(one.getId(), another.getId());
        assertEquals(one.getStatus(), another.getStatus());
        assertEquals(one.isRunnable(), another.isRunnable());
        assertEquals(one.getOutput(), another.getOutput());
        assertEquals(one.getExtra().size(), another.getExtra().size());
        for (String key: one.getExtra().keySet()) {
            assertEquals(one.getExtra().get(key), another.getExtra().get(key));
        }
    }
}

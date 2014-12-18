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
        executable.setStatus(ExecutableStatus.READY);
        HashMap<String, String> extra = new HashMap<>();
        extra.put("test1", "test1");
        extra.put("test2", "test2");
        extra.put("test3", "test3");
        executable.setExtra(extra);
        service.add(executable);
        List<AbstractExecutable> result = service.getAllExecutables();
        assertEquals(1, result.size());
        AbstractExecutable another = service.get(executable.getId());
        assertEquals(executable.getId(), another.getId());
        assertEquals(executable.getStatus(), another.getStatus());
        assertEquals(executable.isRunnable(), another.isRunnable());
        assertEquals(extra.size(), another.getExtra().size());
        for (String key: extra.keySet()) {
            assertEquals(extra.get(key), another.getExtra().get(key));
        }
    }
}

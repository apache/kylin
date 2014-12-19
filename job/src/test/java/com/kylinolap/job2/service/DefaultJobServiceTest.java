package com.kylinolap.job2.service;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.job2.TestExecutable;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultChainedExecutable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
        assertJobEqual(executable, another);

        executable.setStatus(ExecutableStatus.SUCCEED);
        executable.setOutput("test output");
        service.updateJobStatus(executable);
        assertJobEqual(executable, service.getJob(executable.getId()));
    }

    @Test
    public void testDefaultChainedExecutable() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.setId(UUID.randomUUID().toString());
        job.addTask(new TestExecutable());
        job.addTask(new TestExecutable());

        service.addJob(job);
        AbstractExecutable anotherJob = service.getJob(job.getId());
        assertJobEqual(job, anotherJob);
    }



    private static void assertJobEqual(AbstractExecutable one, AbstractExecutable another) {
        assertEquals(one.getId(), another.getId());
        assertEquals(one.getStatus(), another.getStatus());
        assertEquals(one.isRunnable(), another.isRunnable());
        assertEquals(one.getOutput(), another.getOutput());
        assertTrue((one.getExtra() == null && another.getExtra() == null) || (one.getExtra() != null && another.getExtra() != null));
        if (one.getExtra() != null) {
            assertEquals(one.getExtra().size(), another.getExtra().size());
            for (String key : one.getExtra().keySet()) {
                assertEquals(one.getExtra().get(key), another.getExtra().get(key));
            }
        }
        if (one instanceof DefaultChainedExecutable) {
            assertTrue(another instanceof DefaultChainedExecutable);
            List<AbstractExecutable> onesSubs = ((DefaultChainedExecutable) one).getExecutables();
            List<AbstractExecutable> anotherSubs = ((DefaultChainedExecutable) another).getExecutables();
            assertTrue((onesSubs == null && anotherSubs == null) || (onesSubs != null && anotherSubs != null));
            if (onesSubs != null) {
                assertEquals(onesSubs.size(), anotherSubs.size());
                for (int i = 0; i < onesSubs.size(); ++i) {
                    assertJobEqual(onesSubs.get(i), anotherSubs.get(i));
                }
            }
        }
    }
}

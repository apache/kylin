package com.kylinolap.job2.service;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.job2.BaseTestExecutable;
import com.kylinolap.job2.SucceedTestExecutable;
import com.kylinolap.job2.execution.ChainedExecutable;
import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.execution.ExecutableState;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultChainedExecutable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

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
        BaseTestExecutable executable = new SucceedTestExecutable();
        executable.setParam("test1", "test1");
        executable.setParam("test2", "test2");
        executable.setParam("test3", "test3");
        service.addJob(executable);
        List<AbstractExecutable> result = service.getAllExecutables();
        assertEquals(1, result.size());
        AbstractExecutable another = service.getJob(executable.getId());
        assertJobEqual(executable, another);

        service.updateJobStatus(executable.getId(), ExecutableState.RUNNING, "test output");
        assertJobEqual(executable, service.getJob(executable.getId()));
    }

    @Test
    public void testDefaultChainedExecutable() throws Exception {
        DefaultChainedExecutable job = new DefaultChainedExecutable();
        job.addTask(new SucceedTestExecutable());
        job.addTask(new SucceedTestExecutable());

        service.addJob(job);
        assertEquals(2, job.getTasks().size());
        AbstractExecutable anotherJob = service.getJob(job.getId());
        assertEquals(DefaultChainedExecutable.class, anotherJob.getClass());
        assertEquals(2, ((DefaultChainedExecutable) anotherJob).getTasks().size());
        assertJobEqual(job, anotherJob);
    }

    @Test
    public void testValidStateTransfer() throws Exception {
        SucceedTestExecutable job = new SucceedTestExecutable();
        String id = job.getId();
        service.addJob(job);
        service.updateJobStatus(id, ExecutableState.RUNNING);
        service.updateJobStatus(id, ExecutableState.ERROR);
        service.updateJobStatus(id, ExecutableState.READY);
        service.updateJobStatus(id, ExecutableState.RUNNING);
        service.updateJobStatus(id, ExecutableState.STOPPED);
        service.updateJobStatus(id, ExecutableState.READY);
        service.updateJobStatus(id, ExecutableState.RUNNING);
        service.updateJobStatus(id, ExecutableState.SUCCEED);
    }

    @Test
    public void testInvalidStateTransfer(){
        SucceedTestExecutable job = new SucceedTestExecutable();
        service.addJob(job);
        service.updateJobStatus(job.getId(), ExecutableState.RUNNING);
        assertFalse(service.updateJobStatus(job.getId(), ExecutableState.DISCARDED));
    }



    private static void assertJobEqual(Executable one, Executable another) {
        assertEquals(one.getClass(), another.getClass());
        assertEquals(one.getId(), another.getId());
        assertEquals(one.getStatus(), another.getStatus());
        assertEquals(one.isRunnable(), another.isRunnable());
        assertEquals(one.getOutput(), another.getOutput());
        assertTrue((one.getParams() == null && another.getParams() == null) || (one.getParams() != null && another.getParams() != null));
        if (one.getParams() != null) {
            assertEquals(one.getParams().size(), another.getParams().size());
            for (String key : one.getParams().keySet()) {
                assertEquals(one.getParams().get(key), another.getParams().get(key));
            }
        }
        if (one instanceof ChainedExecutable) {
            assertTrue(another instanceof ChainedExecutable);
            List<? extends Executable> onesSubs = ((ChainedExecutable) one).getTasks();
            List<? extends Executable> anotherSubs = ((ChainedExecutable) another).getTasks();
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

package org.apache.kylin.engine.mr.common;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.kylin.job.constant.JobStepStatusEnum;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit tests for class {@link HadoopJobStatusChecker}.
 *
 * @see HadoopJobStatusChecker
 */
public class HadoopJobStatusCheckerTest {

    @Test
    public void testCheckStatusWithNullJob() {
        StringBuilder stringBuilder = new StringBuilder();
        JobStepStatusEnum jobStepStatusEnum = HadoopJobStatusChecker.checkStatus(null, stringBuilder);

        assertEquals(JobStepStatusEnum.WAITING, jobStepStatusEnum);
        assertEquals("Skip status check with empty job id..\n", stringBuilder.toString());

        assertFalse(jobStepStatusEnum.isRunable());
        assertEquals(32, jobStepStatusEnum.getCode());

        assertFalse(jobStepStatusEnum.isComplete());
    }

    @Test
    public void testCheckStatusWithEmptyJobId() throws IOException {
        Job job = new Job(new JobConf(false));
        StringBuilder stringBuilder = new StringBuilder();
        JobStepStatusEnum jobStepStatusEnum = HadoopJobStatusChecker.checkStatus(job, stringBuilder);

        assertEquals(JobStepStatusEnum.WAITING, jobStepStatusEnum);
        assertEquals("Skip status check with empty job id..\n", stringBuilder.toString());

        assertFalse(jobStepStatusEnum.isRunable());
        assertEquals(32, jobStepStatusEnum.getCode());

        assertFalse(jobStepStatusEnum.isComplete());
    }

}
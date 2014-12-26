package com.kylinolap.job2.common;

import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.impl.threadpool.DefaultChainedExecutable;

/**
 * Created by qianzhou on 12/25/14.
 */
public class CommonJob extends DefaultChainedExecutable {
    public CommonJob() {
        super();
    }

    public CommonJob(JobPO job, JobOutputPO jobOutput) {
        super(job, jobOutput);
    }
}

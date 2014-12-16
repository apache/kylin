package com.kylinolap.job2.dao;

import java.util.Collections;
import java.util.List;

/**
 * Created by qianzhou on 12/15/14.
 */
public class JobDao {

    List<JobPO> getJobs() {
        return Collections.emptyList();
    }

    JobPO getJob(String uuid) {
        return null;
    }

    JobPO addJob(JobPO job) {
        return job;
    }

    JobPO updateJob(JobPO job) {
        return job;
    }

    JobPO deleteJob(JobPO job) {
        return job;
    }

}

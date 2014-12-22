package com.kylinolap.job2.service;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobDao;
import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.PersistentException;
import com.kylinolap.job2.execution.ChainedExecutable;
import com.kylinolap.job2.execution.Executable;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultChainedExecutable;
import org.apache.commons.math3.analysis.function.Abs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qianzhou on 12/16/14.
 */
public class DefaultJobService {

    private static final Logger logger = LoggerFactory.getLogger(JobDao.class);
    private static final ConcurrentHashMap<KylinConfig, DefaultJobService> CACHE = new ConcurrentHashMap<KylinConfig, DefaultJobService>();


    private JobDao jobDao;

    public static DefaultJobService getInstance(KylinConfig config) {
        DefaultJobService r = CACHE.get(config);
        if (r == null) {
            r = new DefaultJobService(config);
            CACHE.put(config, r);
            if (CACHE.size() > 1) {
                logger.warn("More than one singleton exist");
            }

        }
        return r;
    }

    private DefaultJobService(KylinConfig config) {
        logger.info("Using metadata url: " + config);
        this.jobDao = JobDao.getInstance(config);
    }

    public void addJob(AbstractExecutable executable) {
        try {
            jobDao.addJob(parseTo(executable));
        } catch (PersistentException e) {
            logger.error("fail to submit job:" + executable.getId(), e);
            throw new RuntimeException(e);
        }
    }

    private void updateJobOutput(String uuid, JobOutputPO output) {
        try {
            jobDao.addOrUpdateJobOutput(uuid, output);
        } catch (PersistentException e) {
            logger.error("fail to update job output id:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    public void deleteJob(AbstractExecutable executable) {
        try {
            jobDao.deleteJob(executable.getId());
        } catch (PersistentException e) {
            logger.error("fail to delete job:" + executable.getId(), e);
            throw new RuntimeException(e);
        }
    }

    public AbstractExecutable getJob(String uuid) {
        try {
            return parseTo(jobDao.getJob(uuid), jobDao.getJobOutput(uuid));
        } catch (PersistentException e) {
            logger.error("fail to get job:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    public ExecutableStatus getJobStatus(String uuid) {
        try {
            return ExecutableStatus.valueOf(jobDao.getJobOutput(uuid).getStatus());
        } catch (PersistentException e) {
            logger.error("fail to get job output:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    public List<AbstractExecutable> getAllExecutables() {
        try {
            return Lists.transform(jobDao.getJobs(), new Function<JobPO, AbstractExecutable>() {
                @Nullable
                @Override
                public AbstractExecutable apply(JobPO input) {
                    try {
                        JobOutputPO jobOutput = jobDao.getJobOutput(input.getUuid());
                        return parseTo(input, jobOutput);
                    } catch (PersistentException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (PersistentException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateJobStatus(String uuid, ExecutableStatus status, String output) {
        JobOutputPO jobOutputPO = new JobOutputPO();
        jobOutputPO.setUuid(uuid);
        jobOutputPO.setContent(output);
        jobOutputPO.setStatus(status.toString());
        updateJobOutput(uuid, jobOutputPO);
    }

    public void updateJobStatus(AbstractExecutable executable) {
        updateJobStatus(executable.getId(), executable.getStatus(), executable.getOutput());
    }

    private JobPO parseTo(AbstractExecutable executable) {
        Preconditions.checkArgument(executable.getId() != null, "please generate unique id");
        JobPO result = new JobPO();
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setExtra(executable.getExtra());
        if (executable instanceof DefaultChainedExecutable) {
            ArrayList<JobPO> tasks = Lists.newArrayList();
            for (AbstractExecutable task : ((DefaultChainedExecutable) executable).getExecutables()) {
                tasks.add(parseTo(task));
            }
            result.setTasks(tasks);
        }
        return result;
    }

    private AbstractExecutable parseTo(JobPO jobPO, JobOutputPO jobOutput) {
        String type = jobPO.getType();
        try {
            Class<? extends AbstractExecutable> clazz = (Class<? extends AbstractExecutable>) Class.forName(type);
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor();
            AbstractExecutable result = constructor.newInstance();
            result.setId(jobPO.getUuid());
            result.setExtra(jobPO.getExtra());
            List<JobPO> tasks = jobPO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof DefaultChainedExecutable);
                for (JobPO subTask: tasks) {
                    ((DefaultChainedExecutable) result).addTask(parseTo(subTask, jobDao.getJobOutput(subTask.getUuid())));
                }
            }
            if (jobOutput != null) {
                result.setStatus(ExecutableStatus.valueOf(jobOutput.getStatus()));
                result.setOutput(jobOutput.getContent());
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("cannot parse this job:" + jobPO.getId(), e);
        } catch (PersistentException e) {
            throw new IllegalArgumentException("cannot parse this job:" + jobPO.getId(), e);
        }
    }

}

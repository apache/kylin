package com.kylinolap.job2.service;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.*;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobDao;
import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.IllegalStateTranferException;
import com.kylinolap.job2.exception.PersistentException;
import com.kylinolap.job2.execution.ExecutableStatus;
import com.kylinolap.job2.execution.StateTransferUtil;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.Set;
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
            jobDao.addJob(getJobPO(executable));
            addJobOutput(executable);
        } catch (PersistentException e) {
            logger.error("fail to submit job:" + executable.getId(), e);
            throw new RuntimeException(e);
        }
    }

    private void addJobOutput(AbstractExecutable executable) throws PersistentException {
        jobDao.addJobOutput(executable.getJobOutput());
        if (executable instanceof DefaultChainedExecutable) {
            for (AbstractExecutable subTask: ((DefaultChainedExecutable) executable).getTasks()) {
                addJob(subTask);
            }
        }
    }

    //for ut
    void deleteJob(AbstractExecutable executable) {
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

    public void resetRunningJobToError(AbstractExecutable executable, String reason) {
        if (executable.getStatus() == ExecutableStatus.RUNNING) {
            updateJobStatus(executable, ExecutableStatus.ERROR, reason);
            if (executable instanceof DefaultChainedExecutable) {
                for (AbstractExecutable subTask : ((DefaultChainedExecutable) executable).getTasks()) {
                    resetRunningJobToError(subTask, reason);
                }
            }
        }
    }

    public void updateJobStatus(AbstractExecutable executable, ExecutableStatus newStatus) {
        updateJobStatus(executable, newStatus, null);
    }

    public void updateJobStatus(AbstractExecutable executable, ExecutableStatus newStatus, String reason) {
        ExecutableStatus oldStatus = executable.getStatus();
        if (!StateTransferUtil.isValidStateTransfer(oldStatus, newStatus)) {
            throw new IllegalStateTranferException("there is no valid state transfer from:" + oldStatus + " to:" + newStatus);
        }
        JobOutputPO output = executable.getJobOutput();
        output.setStatus(newStatus.toString());
        output.setContent(reason);
        try {
            jobDao.updateJobOutput(output);
        } catch (PersistentException e) {
            logger.error("error change job:" + output.getUuid() + " to " + newStatus.toString());
            throw new RuntimeException(e);
        }
    }

    private JobPO getJobPO(AbstractExecutable executable) {
        final JobPO result = executable.getJobPO();
        if (executable instanceof DefaultChainedExecutable) {
            for (AbstractExecutable task: ((DefaultChainedExecutable) executable).getTasks()) {
                result.getTasks().add(getJobPO(task));
            }
        }
        return result;
    }

    private AbstractExecutable parseTo(JobPO jobPO, JobOutputPO jobOutput) {
        String type = jobPO.getType();
        try {
            Class<? extends AbstractExecutable> clazz = (Class<? extends AbstractExecutable>) Class.forName(type);
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor(JobPO.class, JobOutputPO.class);
            AbstractExecutable result = constructor.newInstance(jobPO, jobOutput);
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

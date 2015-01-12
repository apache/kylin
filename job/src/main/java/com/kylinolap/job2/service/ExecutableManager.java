package com.kylinolap.job2.service;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobDao;
import com.kylinolap.job2.dao.JobOutputPO;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.IllegalStateTranferException;
import com.kylinolap.job2.exception.PersistentException;
import com.kylinolap.job2.execution.DefaultOutput;
import com.kylinolap.job2.execution.ExecutableState;
import com.kylinolap.job2.execution.Output;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultChainedExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qianzhou on 12/16/14.
 */
public class ExecutableManager {

    private static final Logger logger = LoggerFactory.getLogger(ExecutableManager.class);
    private static final ConcurrentHashMap<KylinConfig, ExecutableManager> CACHE = new ConcurrentHashMap<KylinConfig, ExecutableManager>();

    private JobDao jobDao;

    public static ExecutableManager getInstance(KylinConfig config) {
        ExecutableManager r = CACHE.get(config);
        if (r == null) {
            r = new ExecutableManager(config);
            CACHE.put(config, r);
            if (CACHE.size() > 1) {
                logger.warn("More than one singleton exist");
            }

        }
        return r;
    }

    private ExecutableManager(KylinConfig config) {
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
        JobOutputPO jobOutputPO = new JobOutputPO();
        jobOutputPO.setUuid(executable.getId());
        jobDao.addJobOutput(jobOutputPO);
        if (executable instanceof DefaultChainedExecutable) {
            for (AbstractExecutable subTask: ((DefaultChainedExecutable) executable).getTasks()) {
                addJobOutput(subTask);
            }
        }
    }

    //for ut
    public void deleteJob(String jobId) {
        try {
            jobDao.deleteJob(jobId);
        } catch (PersistentException e) {
            logger.error("fail to delete job:" + jobId, e);
            throw new RuntimeException(e);
        }
    }

    public AbstractExecutable getJob(String uuid) {
        try {
            return parseTo(jobDao.getJob(uuid));
        } catch (PersistentException e) {
            logger.error("fail to get job:" + uuid, e);
            throw new RuntimeException(e);
        }
    }

    public Output getOutput(String uuid) {
        try {
            final JobOutputPO jobOutput = jobDao.getJobOutput(uuid);
            Preconditions.checkArgument(jobOutput != null, "there is no related output for job id:" + uuid);
            final DefaultOutput result = new DefaultOutput();
            result.setExtra(jobOutput.getInfo());
            result.setState(ExecutableState.valueOf(jobOutput.getStatus()));
            result.setVerboseMsg(jobOutput.getContent());
            result.setLastModified(jobOutput.getLastModified());
            return result;
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
                        return parseTo(input);
                }
            });
        } catch (PersistentException e) {
            logger.error("error get All Jobs", e);
            throw new RuntimeException(e);
        }
    }

    public List<String> getAllJobIds() {
        try {
            return jobDao.getJobIds();
        } catch (PersistentException e) {
            logger.error("error get All Job Ids", e);
            throw new RuntimeException(e);
        }
    }

    public void updateAllRunningJobsToError() {
        try {
            final List<JobOutputPO> jobOutputs = jobDao.getJobOutputs();
            for (JobOutputPO jobOutputPO: jobOutputs) {
                if (jobOutputPO.getStatus().equalsIgnoreCase(ExecutableState.RUNNING.toString())) {
                    jobOutputPO.setStatus(ExecutableState.ERROR.toString());
                    jobDao.updateJobOutput(jobOutputPO);
                }
            }
        } catch (PersistentException e) {
            logger.error("error reset job status from RUNNING to ERROR", e);
            throw new RuntimeException(e);
        }
    }

    public void resumeJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        updateJobOutput(jobId, ExecutableState.READY, null, null);
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (task.getStatus() == ExecutableState.ERROR) {
                    updateJobOutput(task.getId(), ExecutableState.READY, null, null);
                    break;
                }
            }
        }
    }

    public void discardJob(String jobId) {
        AbstractExecutable job = getJob(jobId);
        if (job instanceof DefaultChainedExecutable) {
            List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
            for (AbstractExecutable task : tasks) {
                if (!task.getStatus().isFinalState()) {
                    updateJobOutput(task.getId(), ExecutableState.DISCARDED, null, null);
                }
            }
        }
        updateJobOutput(jobId, ExecutableState.DISCARDED, null, null);
    }

    public void updateJobOutput(String jobId, ExecutableState newStatus, Map<String, String> info, String output) {
        try {
            final JobOutputPO jobOutput = jobDao.getJobOutput(jobId);
            Preconditions.checkArgument(jobOutput != null, "there is no related output for job id:" + jobId);
            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
            if (newStatus != null && oldStatus != newStatus) {
                if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
                    throw new IllegalStateTranferException("there is no valid state transfer from:" + oldStatus + " to:" + newStatus);
                }
                jobOutput.setStatus(newStatus.toString());
            }
            if (info != null) {
                jobOutput.setInfo(info);
            }
            if (output != null) {
                jobOutput.setContent(output);
            }
            jobDao.updateJobOutput(jobOutput);
            logger.info("job id:" + jobId + " from " + oldStatus + " to " + newStatus);
        } catch (PersistentException e) {
            logger.error("error change job:" + jobId + " to " + newStatus.toString());
            throw new RuntimeException(e);
        }
    }

//    public boolean updateJobStatus(String jobId, ExecutableState newStatus) {
//        try {
//            final JobOutputPO jobOutput = jobDao.getJobOutput(jobId);
//            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
//            if (oldStatus == newStatus) {
//                return true;
//            }
//            if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
//                throw new RuntimeException("there is no valid state transfer from:" + oldStatus + " to:" + newStatus);
//            }
//            jobOutput.setStatus(newStatus.toString());
//            jobDao.updateJobOutput(jobOutput);
//            logger.info("job id:" + jobId + " from " + oldStatus + " to " + newStatus);
//            return true;
//        } catch (PersistentException e) {
//            logger.error("error change job:" + jobId + " to " + newStatus.toString());
//            throw new RuntimeException(e);
//        }
//    }
//
//    public boolean updateJobStatus(String jobId, ExecutableState newStatus, String output) {
//        try {
//            final JobOutputPO jobOutput = jobDao.getJobOutput(jobId);
//            ExecutableState oldStatus = ExecutableState.valueOf(jobOutput.getStatus());
//            if (oldStatus == newStatus) {
//                return true;
//            }
//            if (!ExecutableState.isValidStateTransfer(oldStatus, newStatus)) {
//                throw new RuntimeException("there is no valid state transfer from:" + oldStatus + " to:" + newStatus);
//            }
//            jobOutput.setStatus(newStatus.toString());
//            jobOutput.setContent(output);
//            jobDao.updateJobOutput(jobOutput);
//            logger.info("job id:" + jobId + " from " + oldStatus + " to " + newStatus);
//            return true;
//        } catch (PersistentException e) {
//            logger.error("error change job:" + jobId + " to " + newStatus.toString());
//            throw new RuntimeException(e);
//        }
//    }

    public void addJobInfo(String id, Map<String, String> info) {
        if (info == null) {
            return;
        }
        try {
            JobOutputPO output = jobDao.getJobOutput(id);
            Preconditions.checkArgument(output != null, "there is no related output for job id:" + id);
            output.getInfo().putAll(info);
            jobDao.updateJobOutput(output);
        } catch (PersistentException e) {
            logger.error("error update job info, id:" + id + "  info:" + info.toString());
            throw new RuntimeException(e);
        }
    }

    public void addJobInfo(String id, String key, String value) {
        Map<String, String> info = Maps.newHashMap();
        info.put(key, value);
        addJobInfo(id, info);
    }

    private void stopJob(AbstractExecutable job) {
        final ExecutableState status = job.getStatus();
        if (status == ExecutableState.RUNNING) {
            updateJobOutput(job.getId(), ExecutableState.STOPPED, null, null);
            if (job instanceof DefaultChainedExecutable) {
                final List<AbstractExecutable> tasks = ((DefaultChainedExecutable) job).getTasks();
                for (AbstractExecutable task: tasks) {
                    if (task.getStatus() == ExecutableState.RUNNING) {
                        stopJob(task);
                        break;
                    }
                }
            }
        } else {
            updateJobOutput(job.getId(), ExecutableState.STOPPED, null, null);
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

    private AbstractExecutable parseTo(JobPO jobPO) {
        if (jobPO == null) {
            return null;
        }
        String type = jobPO.getType();
        try {
            Class<? extends AbstractExecutable> clazz = (Class<? extends AbstractExecutable>) Class.forName(type);
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor(JobPO.class);
            AbstractExecutable result = constructor.newInstance(jobPO);
            List<JobPO> tasks = jobPO.getTasks();
            if (tasks != null && !tasks.isEmpty()) {
                Preconditions.checkArgument(result instanceof DefaultChainedExecutable);
                for (JobPO subTask: tasks) {
                    ((DefaultChainedExecutable) result).addTask(parseTo(subTask));
                }
            }
            return result;
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("cannot parse this job:" + jobPO.getId(), e);
        }
    }

}

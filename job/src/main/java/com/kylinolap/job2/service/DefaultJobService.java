package com.kylinolap.job2.service;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.job2.dao.JobDao;
import com.kylinolap.job2.dao.JobPO;
import com.kylinolap.job2.exception.ExecuteException;
import com.kylinolap.job2.exception.PersistentException;
import com.kylinolap.job2.execution.ExecuteStatus;
import com.kylinolap.job2.impl.threadpool.AbstractExecutable;
import com.kylinolap.job2.impl.threadpool.DefaultChainedExecutable;
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

    public boolean add(AbstractExecutable executable) {
        try {
            jobDao.addJob(parseTo(executable));
            return true;
        } catch (PersistentException e) {
            logger.error("fail to submit job:" + executable.getId(), e);
            return false;
        }
    }

    public boolean update(AbstractExecutable executable) {
        try {
            jobDao.updateJob(parseTo(executable));
            return true;
        } catch (PersistentException e) {
            logger.error("fail to stop job:" + executable.getId(), e);
            return false;
        }
    }

    public boolean delete(AbstractExecutable executable) {
        try {
            jobDao.deleteJob(executable.getId());
            return true;
        } catch (PersistentException e) {
            logger.error("fail to delete job:" + executable.getId(), e);
            return false;
        }
    }

    public AbstractExecutable get(String uuid) {
        try {
            return parseTo(jobDao.getJob(uuid));
        } catch (PersistentException e) {
            logger.error("fail to get job:" + uuid, e);
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
            throw new RuntimeException(e);
        }
    }

    private JobPO parseTo(AbstractExecutable executable) {
        Preconditions.checkArgument(executable.getId() != null, "please generate unique id");
        JobPO result = new JobPO();
        result.setAsync(executable.isAsync());
        result.setUuid(executable.getId());
        result.setType(executable.getClass().getName());
        result.setStatus(executable.getStatus().toString());
        result.setExtra(executable.getExtra());
        if (executable instanceof DefaultChainedExecutable) {
            ArrayList<JobPO> tasks = Lists.<JobPO>newArrayList();
            for (AbstractExecutable task : ((DefaultChainedExecutable) executable).getExecutables()) {
                tasks.add(parseTo(task));
            }
            result.setTasks(tasks);
        }
        return result;
    }

    private AbstractExecutable parseTo(JobPO jobPO) {
        String type = jobPO.getType();
        try {
            Class<? extends AbstractExecutable> clazz = (Class<? extends AbstractExecutable>) Class.forName(type);
            Constructor<? extends AbstractExecutable> constructor = clazz.getConstructor();
            AbstractExecutable result = constructor.newInstance();
            result.setAsync(jobPO.isAsync());
            result.setStatus(ExecuteStatus.valueOf(jobPO.getStatus()));
            result.setId(jobPO.getUuid());
            result.setExtra(jobPO.getExtra());
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

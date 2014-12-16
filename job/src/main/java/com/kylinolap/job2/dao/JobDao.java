package com.kylinolap.job2.dao;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.job2.exception.PersistentException;
import com.kylinolap.metadata.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qianzhou on 12/15/14.
 */
public class JobDao {

    private static final Serializer<JobPO> JOB_SERIALIZER = new JsonSerializer<JobPO>(JobPO.class);
    private static final Logger logger = LoggerFactory.getLogger(JobDao.class);
    private static final ConcurrentHashMap<KylinConfig, JobDao> CACHE = new ConcurrentHashMap<KylinConfig, JobDao>();
    public static final String JOB_PATH_ROOT = "/execute";

    private ResourceStore store;

    public static JobDao getInstance(KylinConfig config) {
        JobDao r = CACHE.get(config);
        if (r == null) {
            r = new JobDao(config);
            CACHE.put(config, r);
            if (CACHE.size() > 1) {
                logger.warn("More than one singleton exist");
            }

        }
        return r;
    }

    private JobDao(KylinConfig config) {
        logger.info("Using metadata url: " + config);
        this.store = MetadataManager.getInstance(config).getStore();
    }

    private String pathOfJob(JobPO job) {
        return pathOfJob(job.getUuid());
    }
    private String pathOfJob(String uuid) {
        return JOB_PATH_ROOT + "/" + uuid;
    }

    private JobPO readJobResource(String path) throws IOException {
        return store.getResource(path, JobPO.class, JOB_SERIALIZER);
    }

    private void writeJobResource(String path, JobPO job) throws IOException {
        store.putResource(path, job, JOB_SERIALIZER);
    }

    public List<JobPO> getJobs() throws PersistentException {
        try {
            ArrayList<String> resources = store.listResources(JOB_PATH_ROOT);
            if (resources == null) {
                return Collections.emptyList();
            }
            ArrayList<JobPO> result = new ArrayList<JobPO>(resources.size());
            for (String path : resources) {
                result.add(readJobResource(path));
            }
            return result;
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public JobPO getJob(String uuid) throws PersistentException {
        try {
            return readJobResource(pathOfJob(uuid));
        } catch (IOException e) {
            logger.error("error get job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public JobPO addJob(JobPO job) throws PersistentException {
        try {
            if (getJob(job.getUuid()) != null) {
                throw new IllegalArgumentException("job id:" + job.getUuid() + " already exists");
            }
            writeJobResource(pathOfJob(job), job);
            return job;
        } catch (IOException e) {
            logger.error("error save job:" + job.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public JobPO updateJob(JobPO job) throws PersistentException {
        try {
            JobPO existedJob = getJob(job.getUuid());
            if (existedJob == null) {
                throw new IllegalArgumentException("job id:" + job.getUuid() + " does not exists");
            }
            job.setLastModified(existedJob.getLastModified());
            writeJobResource(pathOfJob(job), job);
        } catch (IOException e) {
            logger.error("error save job:" + job.getUuid(), e);
            throw new PersistentException(e);
        }
        return job;
    }

    public String deleteJob(String uuid) throws PersistentException {
        try {
            store.deleteResource(pathOfJob(uuid));
            return uuid;
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

}

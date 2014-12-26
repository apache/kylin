package com.kylinolap.job2.dao;

import com.google.common.base.Preconditions;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.job2.exception.PersistentException;
import com.kylinolap.job2.execution.ExecutableStatus;
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
    private static final Serializer<JobOutputPO> JOB_OUTPUT_SERIALIZER = new JsonSerializer<JobOutputPO>(JobOutputPO.class);
    private static final Logger logger = LoggerFactory.getLogger(JobDao.class);
    private static final ConcurrentHashMap<KylinConfig, JobDao> CACHE = new ConcurrentHashMap<KylinConfig, JobDao>();
    public static final String JOB_PATH_ROOT = "/execute";
    public static final String JOB_OUTPUT_ROOT = "/execute_output";

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

    private String pathOfJobOutput(String uuid) {
        return JOB_OUTPUT_ROOT + "/" + uuid;
    }

    private JobPO readJobResource(String path) throws IOException {
        return store.getResource(path, JobPO.class, JOB_SERIALIZER);
    }

    private void writeJobResource(String path, JobPO job) throws IOException {
        store.putResource(path, job, JOB_SERIALIZER);
    }

    private JobOutputPO readJobOutputResource(String path) throws IOException {
        return store.getResource(path, JobOutputPO.class, JOB_OUTPUT_SERIALIZER);
    }

    private long writeJobOutputResource(String path, JobOutputPO output) throws IOException {
        return store.putResource(path, output, JOB_OUTPUT_SERIALIZER);
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

    public void deleteJob(String uuid) throws PersistentException {
        try {
            store.deleteResource(pathOfJob(uuid));
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public JobOutputPO getJobOutput(String uuid) throws PersistentException {
        try {
            JobOutputPO result = readJobOutputResource(pathOfJobOutput(uuid));
            if (result == null) {
                result = new JobOutputPO();
                result.setStatus(ExecutableStatus.READY.toString());
                result.setUuid(uuid);
                return result;
            }
            return result;
        } catch (IOException e) {
            logger.error("error get job output id:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public void addJobOutput(JobOutputPO output) throws PersistentException {
        try {
            output.setLastModified(0);
            writeJobOutputResource(pathOfJobOutput(output.getUuid()), output);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void updateJobOutput(JobOutputPO output) throws PersistentException {
        try {
            Preconditions.checkArgument(output.getLastModified() > 0, "timestamp should be greater than 0 inf");
            final long ts = writeJobOutputResource(pathOfJobOutput(output.getUuid()), output);
            output.setLastModified(ts);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void deleteJobOutput(String uuid) throws PersistentException {
        try {
            store.deleteResource(pathOfJobOutput(uuid));
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }
}

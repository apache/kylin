/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.job.dao;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ContentReader;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.WriteConflictException;
import org.apache.kylin.common.util.AutoReadWriteLock;
import org.apache.kylin.job.exception.PersistentException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.cachesync.CaseInsensitiveStringCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 */
public class ExecutableDao {

    private static final Serializer<ExecutablePO> JOB_SERIALIZER = new JsonSerializer<ExecutablePO>(ExecutablePO.class);
    private static final Serializer<ExecutableOutputPO> JOB_OUTPUT_SERIALIZER = new JsonSerializer<ExecutableOutputPO>(
            ExecutableOutputPO.class);
    private static final Logger logger = LoggerFactory.getLogger(ExecutableDao.class);

    public static ExecutableDao getInstance(KylinConfig config) {
        return config.getManager(ExecutableDao.class);
    }

    // called by reflection
    static ExecutableDao newInstance(KylinConfig config) throws IOException {
        return new ExecutableDao(config);
    }

    // ============================================================================

    private ResourceStore store;

    private CaseInsensitiveStringCache<ExecutablePO> executableDigestMap;

    private CaseInsensitiveStringCache<ExecutableOutputPO> executableOutputDigestMap;

    private CachedCrudAssist<ExecutablePO> executableDigestCrud;

    private CachedCrudAssist<ExecutableOutputPO> executableOutputDigestCrud;

    private AutoReadWriteLock executableDigestMapLock = new AutoReadWriteLock();

    private AutoReadWriteLock executableOutputDigestMapLock = new AutoReadWriteLock();

    private ExecutableDao(KylinConfig config) throws IOException {
        logger.info("Using metadata url: {}", config);
        this.store = ResourceStore.getStore(config);
        this.executableDigestMap = new CaseInsensitiveStringCache<>(config, "execute");
        this.executableDigestCrud = new CachedCrudAssist<ExecutablePO>(store, ResourceStore.EXECUTE_RESOURCE_ROOT, "",
                ExecutablePO.class, executableDigestMap, false) {
            @Override
            public ExecutablePO reloadAt(String path) {
                try {
                    ExecutablePO executablePO = readJobResource(path);
                    if (executablePO == null) {
                        logger.warn("No job found at {}, returning null", path);
                        executableDigestMap.removeLocal(resourceName(path));
                        return null;
                    }

                    // create a digest
                    ExecutablePO digestExecutablePO = new ExecutablePO();
                    digestExecutablePO.setUuid(executablePO.getUuid());
                    digestExecutablePO.setName(executablePO.getName());
                    digestExecutablePO.setLastModified(executablePO.getLastModified());
                    digestExecutablePO.setType(executablePO.getType());
                    digestExecutablePO.setParams(executablePO.getParams());
                    executableDigestMap.putLocal(resourceName(path), digestExecutablePO);
                    return digestExecutablePO;
                } catch (Exception e) {
                    throw new IllegalStateException("Error loading execute at " + path, e);
                }
            }

            @Override
            protected ExecutablePO initEntityAfterReload(ExecutablePO entity, String resourceName) {
                return entity;
            }
        };
        this.executableDigestCrud.setCheckCopyOnWrite(true);
        this.executableDigestCrud.reloadAll();

        this.executableOutputDigestMap = new CaseInsensitiveStringCache<>(config, "execute_output");
        this.executableOutputDigestCrud = new CachedCrudAssist<ExecutableOutputPO>(store,
                ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, "", ExecutableOutputPO.class, executableOutputDigestMap,
                false) {
            @Override
            public void reloadAll() throws IOException {
                logger.debug("Reloading execute_output from " + ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT);
                executableOutputDigestMap.clear();

                NavigableSet<String> paths = store.listResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT);

                if (paths != null) {
                    for (String path : paths) {
                        if (!isTaskExecutableOutput(resourceName(path)))
                            reloadAt(path);
                    }

                    logger.debug("Loaded {} execute_output digest(s) out of {} resource",
                            executableOutputDigestMap.size(), paths.size());
                }
            }

            @Override
            public ExecutableOutputPO reloadAt(String path) {
                try {
                    ExecutableOutputPO executableOutputPO = readJobOutputResource(path);
                    if (executableOutputPO == null) {
                        logger.warn("No job output found at {}, returning null", path);
                        executableOutputDigestMap.removeLocal(resourceName(path));
                        return null;
                    }

                    // create a digest
                    ExecutableOutputPO digestExecutableOutputPO = new ExecutableOutputPO();
                    digestExecutableOutputPO.setUuid(executableOutputPO.getUuid());
                    digestExecutableOutputPO.setLastModified(executableOutputPO.getLastModified());
                    digestExecutableOutputPO.setStatus(executableOutputPO.getStatus());
                    executableOutputDigestMap.putLocal(resourceName(path), digestExecutableOutputPO);
                    return digestExecutableOutputPO;
                } catch (Exception e) {
                    throw new IllegalStateException("Error loading execute at " + path, e);
                }
            }

            @Override
            protected ExecutableOutputPO initEntityAfterReload(ExecutableOutputPO entity, String resourceName) {
                return entity;
            }
        };
        this.executableOutputDigestCrud.setCheckCopyOnWrite(true);
        this.executableOutputDigestCrud.reloadAll();
        Broadcaster.getInstance(config).registerListener(new JobSyncListener(), "execute");
        Broadcaster.getInstance(config).registerListener(new JobOutputSyncListener(), "execute_output");
    }

    /**
     * Length of java.util.UUID's string representation is always 36.
     */
    private static final int UUID_STRING_REPRESENTATION_LENGTH = 36;

    /**
     * <pre>
     *    Backgroud :
     * 1. Each Executable has id, and id should be unique, we use java.util.UUID to create id of Executable.
     * 2. 36(UUID_STRING_REPRESENTATION_LENGTH) is a magic number, and it is the length of string of java.util.UUID.toString(). It can verified this simply by `System.out.println(UUID.randomUUID().toString().length());`
     * 3. All subtask of a ChainedExecutable is also a Executable, its id is a string which length is 39 (36 + 3). See DefaultChainedExecutable#addTask.
     * 4. Any other Executable's id is a String created by UUID.toString(), so its length is 36.
     * 5. This method may be a bit fragile/confusing because it depend on specific implementation of subclass of Executable.
     * </pre>
     *
     * @see DefaultChainedExecutable#addTask(AbstractExecutable)
     * @see AbstractExecutable#AbstractExecutable()
     *
     * @param id id of a Executable (mostly it is a UUID)
     * @return true if the job is a subtask of a ChainedExecutable; else return false
     */
    private boolean isTaskExecutableOutput(String id) {
        return id.length() > UUID_STRING_REPRESENTATION_LENGTH;
    }

    private class JobSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            try (AutoReadWriteLock.AutoLock l = executableDigestMapLock.lockForWrite()) {
                if (event == Broadcaster.Event.DROP)
                    executableDigestMap.removeLocal(cacheKey);
                else
                    executableDigestCrud.reloadQuietly(cacheKey);
            }
        }
    }

    private class JobOutputSyncListener extends Broadcaster.Listener {
        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            try (AutoReadWriteLock.AutoLock l = executableOutputDigestMapLock.lockForWrite()) {
                if (!isTaskExecutableOutput(cacheKey)) {
                    if (event == Broadcaster.Event.DROP)
                        executableOutputDigestMap.removeLocal(cacheKey);
                    else
                        executableOutputDigestCrud.reloadQuietly(cacheKey);
                }
            }
        }
    }

    private String pathOfJob(ExecutablePO job) {
        return pathOfJob(job.getUuid());
    }

    public static String pathOfJob(String uuid) {
        return ResourceStore.EXECUTE_RESOURCE_ROOT + "/" + uuid;
    }

    public static String pathOfJobOutput(String uuid) {
        return ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT + "/" + uuid;
    }

    private ExecutablePO readJobResource(String path) throws IOException {
        return store.getResource(path, JOB_SERIALIZER);
    }

    private void writeJobResource(String path, ExecutablePO job) throws IOException {
        store.checkAndPutResource(path, job, JOB_SERIALIZER);
    }

    private ExecutableOutputPO readJobOutputResource(String path) throws IOException {
        return store.getResource(path, JOB_OUTPUT_SERIALIZER);
    }

    private void writeJobOutputResource(String path, ExecutableOutputPO output) throws IOException {
        store.checkAndPutResource(path, output, JOB_OUTPUT_SERIALIZER);
    }

    public List<ExecutableOutputPO> getJobOutputs() throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, JOB_OUTPUT_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<ExecutableOutputPO> getJobOutputs(long timeStart, long timeEndExclusive) throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_OUTPUT_RESOURCE_ROOT, false,
                    new ResourceStore.VisitFilter(timeStart, timeEndExclusive),
                    new ContentReader(JOB_OUTPUT_SERIALIZER));
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public ExecutableOutputPO getJobOutputDigest(String uuid) {
        return executableOutputDigestMap.get(uuid);
    }

    public List<ExecutableOutputPO> getJobOutputDigests(long timeStart, long timeEndExclusive) {
        List<ExecutableOutputPO> jobOutputDigests = Lists.newArrayList();
        for (ExecutableOutputPO po : executableOutputDigestMap.values()) {
            if (po.getLastModified() >= timeStart && po.getLastModified() < timeEndExclusive)
                jobOutputDigests.add(po);
        }
        return jobOutputDigests;
    }

    public List<ExecutablePO> getJobs() throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_RESOURCE_ROOT, JOB_SERIALIZER);
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public List<ExecutablePO> getJobs(long timeStart, long timeEndExclusive) throws PersistentException {
        try {
            return store.getAllResources(ResourceStore.EXECUTE_RESOURCE_ROOT, false,
                    new ResourceStore.VisitFilter(timeStart, timeEndExclusive), new ContentReader(JOB_SERIALIZER));
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO getJobDigest(String uuid) {
        return executableDigestMap.get(uuid);
    }

    public List<ExecutablePO> getJobDigests(long timeStart, long timeEndExclusive) {
        List<ExecutablePO> jobDigests = Lists.newArrayList();
        for (ExecutablePO po : executableDigestMap.values()) {
            if (po.getLastModified() >= timeStart && po.getLastModified() < timeEndExclusive)
                jobDigests.add(po);
        }
        return jobDigests;
    }

    public List<String> getJobIdsInCache() {
        Set<String> idSet = executableDigestMap.keySet();
        return Lists.newArrayList(idSet);
    }

    public List<String> getJobIds() throws PersistentException {
        try {
            NavigableSet<String> resources = store.listResources(ResourceStore.EXECUTE_RESOURCE_ROOT);
            if (resources == null) {
                return Collections.emptyList();
            }
            ArrayList<String> result = Lists.newArrayListWithExpectedSize(resources.size());
            for (String path : resources) {
                result.add(path.substring(path.lastIndexOf("/") + 1));
            }
            return result;
        } catch (IOException e) {
            logger.error("error get all Jobs:", e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO getJob(String uuid) throws PersistentException {
        try {
            return readJobResource(pathOfJob(uuid));
        } catch (IOException e) {
            logger.error("error get job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO addJob(ExecutablePO job) throws PersistentException {
        try {
            if (getJob(job.getUuid()) != null) {
                throw new IllegalArgumentException("job id:" + job.getUuid() + " already exists");
            }
            writeJobResource(pathOfJob(job), job);
            executableDigestMap.put(job.getId(), job);
            return job;
        } catch (IOException e) {
            logger.error("error save job:" + job.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public ExecutablePO updateJob(ExecutablePO job) throws PersistentException {
        try {
            if (getJob(job.getUuid()) == null) {
                throw new IllegalArgumentException("job id:" + job.getUuid() + " does not exist");
            }
            writeJobResource(pathOfJob(job), job);
            executableDigestMap.put(job.getId(), job);
            return job;
        } catch (IOException e) {
            logger.error("error update job:" + job.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void deleteJob(String uuid) throws PersistentException {
        try {
            ExecutablePO executablePO = getJob(uuid);
            store.deleteResource(pathOfJob(uuid));
            executableDigestMap.remove(uuid);
            removeJobOutput(executablePO);
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    private void removeJobOutput(ExecutablePO executablePO) {
        List<String> toDeletePaths = Lists.newArrayList();
        try {
            toDeletePaths.add(pathOfJobOutput(executablePO.getUuid()));
            for (ExecutablePO task : executablePO.getTasks()) {
                toDeletePaths.add(pathOfJobOutput(task.getUuid()));
            }
            for (String path : toDeletePaths) {
                store.deleteResource(path);
            }
        } catch (Exception e) {
            logger.warn("error delete job output:" + executablePO.getUuid(), e);
        }
    }

    public ExecutableOutputPO getJobOutput(String uuid) throws PersistentException {
        ExecutableOutputPO result = null;
        try {
            result = readJobOutputResource(pathOfJobOutput(uuid));
            if (result == null) {
                result = new ExecutableOutputPO();
                result.setUuid(uuid);
                return result;
            }
            return result;
        } catch (IOException e) {
            logger.error("error get job output id:" + uuid, e);
            if (e.getCause() instanceof FileNotFoundException) {
                result = new ExecutableOutputPO();
                result.setUuid(uuid);
                result.setStatus(ExecutableState.SUCCEED.name());
                return result;
            } else {
                throw new PersistentException(e);
            }
        }
    }

    public void addJobOutput(ExecutableOutputPO output) throws PersistentException {
        try {
            output.setLastModified(0);
            writeJobOutputResource(pathOfJobOutput(output.getUuid()), output);
            if (!isTaskExecutableOutput(output.getUuid()))
                executableOutputDigestMap.put(output.getUuid(), output);
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void updateJobOutput(ExecutableOutputPO output) throws PersistentException {
        try {
            int retry = 7;
            while (retry-- > 0) {
                try {
                    writeJobOutputResource(pathOfJobOutput(output.getUuid()), output);
                    if (!isTaskExecutableOutput(output.getUuid()))
                        executableOutputDigestMap.put(output.getUuid(), output);
                    return;
                } catch (WriteConflictException e) {
                    if (retry <= 0) {
                        logger.error("Retry is out, till got error, abandoning...", e);
                        throw e;
                    }
                    logger.warn("Write conflict to update  job output id:" +  output.getUuid() + " retry remaining " + retry
                            + ", will retry...");
                }
            }
        } catch (IOException e) {
            logger.error("error update job output id:" + output.getUuid(), e);
            throw new PersistentException(e);
        }
    }

    public void deleteJobOutput(String uuid) throws PersistentException {
        try {
            store.deleteResource(pathOfJobOutput(uuid));
            if (!isTaskExecutableOutput(uuid))
                executableOutputDigestMap.remove(uuid);
        } catch (IOException e) {
            logger.error("error delete job:" + uuid, e);
            throw new PersistentException(e);
        }
    }

    public void reloadAll() throws IOException {
        try (AutoReadWriteLock.AutoLock lock = executableDigestMapLock.lockForWrite()) {
            executableDigestCrud.reloadAll();
        }
        try (AutoReadWriteLock.AutoLock lock = executableOutputDigestMapLock.lockForWrite()) {
            executableOutputDigestCrud.reloadAll();
        }
    }

    public void syncDigestsOfJob(String uuid) throws PersistentException {
        ExecutablePO job = getJob(uuid);
        ExecutablePO jobDigest = getJobDigest(uuid);

        if (job == null && jobDigest != null) {
            executableDigestMap.remove(uuid);
        } else if (job != null && jobDigest == null) {
            executableDigestMap.put(uuid, job);
        }

        ExecutableOutputPO jobOutput = getJobOutput(uuid);
        ExecutableOutputPO jobOutputDigest = getJobOutputDigest(uuid);

        if (jobOutput == null && jobOutputDigest != null) {
            executableOutputDigestMap.remove(uuid);
        } else if (jobOutput != null && jobOutputDigest == null) {
            executableOutputDigestMap.put(uuid, jobOutput);
        }
    }
}

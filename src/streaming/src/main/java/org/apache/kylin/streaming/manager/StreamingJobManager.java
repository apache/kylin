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

package org.apache.kylin.streaming.manager;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;
import org.apache.kylin.streaming.util.JobKiller;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobManager {

    private String project;

    private KylinConfig config;

    private CachedCrudAssist<StreamingJobMeta> crud;

    private StreamingJobManager(KylinConfig config, String project) {
        this.project = project;
        this.config = config;
        String resourceRootPath = "/" + project + ResourceStore.STREAMING_RESOURCE_ROOT;
        this.crud = new CachedCrudAssist<StreamingJobMeta>(getStore(), resourceRootPath, "", StreamingJobMeta.class) {
            @Override
            protected StreamingJobMeta initEntityAfterReload(StreamingJobMeta entity, String resourceName) {
                entity.setProject(project);
                return entity;
            }
        };
    }

    public static StreamingJobManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, StreamingJobManager.class);
    }

    // called by reflection
    static StreamingJobManager newInstance(KylinConfig config, String project) {
        return new StreamingJobManager(config, project);
    }

    public StreamingJobMeta getStreamingJobByUuid(String uuid) {
        if (StringUtils.isEmpty(uuid)) {
            return null;
        }
        return crud.get(uuid);
    }

    public void createStreamingJob(NDataModel model) {
        createStreamingJob(model, JobTypeEnum.STREAMING_BUILD);
        createStreamingJob(model, JobTypeEnum.STREAMING_MERGE);
    }

    public void createStreamingJob(NDataModel model, JobTypeEnum jobType) {
        StreamingJobMeta job = StreamingJobMeta.create(model, JobStatusEnum.STOPPED, jobType);
        crud.save(job);
    }

    public StreamingJobMeta copy(StreamingJobMeta jobMeta) {
        return crud.copyBySerialization(jobMeta);
    }

    public StreamingJobMeta updateStreamingJob(String jobId, NStreamingJobUpdater updater) {
        StreamingJobMeta cached = getStreamingJobByUuid(jobId);
        if (cached == null) {
            return null;
        }
        StreamingJobMeta copy = copy(cached);
        updater.modify(copy);
        return crud.save(copy);
    }

    public void deleteStreamingJob(String uuid) {
        StreamingJobMeta job = getStreamingJobByUuid(uuid);
        if (job == null) {
            log.warn("Dropping streaming job {} doesn't exists", uuid);
            return;
        }
        log.info("deleteStreamingJob:" + uuid);
        crud.delete(uuid);
    }

    public List<StreamingJobMeta> listAllStreamingJobMeta() {
        return crud.listAll();
    }

    public void destroyAllProcess() {
        listAllStreamingJobMeta().stream().forEach(JobKiller::killProcess);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(config);
    }

    public interface NStreamingJobUpdater {
        void modify(StreamingJobMeta copyForWrite);
    }

}

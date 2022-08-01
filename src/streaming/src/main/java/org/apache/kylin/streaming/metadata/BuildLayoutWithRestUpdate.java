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

package org.apache.kylin.streaming.metadata;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.engine.spark.job.BuildLayoutWithUpdate;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.streaming.request.LayoutUpdateRequest;
import org.apache.kylin.streaming.rest.RestSupport;
import org.apache.kylin.streaming.util.JobExecutionIdHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import lombok.val;

public class BuildLayoutWithRestUpdate extends BuildLayoutWithUpdate {
    protected static final Logger logger = LoggerFactory.getLogger(BuildLayoutWithRestUpdate.class);

    private JobTypeEnum jobType;

    public BuildLayoutWithRestUpdate(JobTypeEnum jobType) {
        this.jobType = jobType;
    }

    protected void updateLayouts(KylinConfig config, String project, String dataflowId,
            final List<NDataLayout> layouts) {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        if (conf.isUTEnv()) {
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                NDataflowUpdate update = new NDataflowUpdate(dataflowId);
                update.setToAddOrUpdateLayouts(layouts.toArray(new NDataLayout[0]));
                NDataflowManager.getInstance(conf, project).updateDataflow(update);
                return 0;
            }, project);
        } else {
            callUpdateLayouts(conf, project, dataflowId, layouts);
        }
    }

    public void callUpdateLayouts(KylinConfig conf, String project, String dataflowId,
            final List<NDataLayout> layouts) {
        String url = "/streaming_jobs/dataflow/layout";
        List<NDataSegDetails> segDetails = layouts.stream().map(item -> item.getSegDetails())
                .collect(Collectors.toList());
        LayoutUpdateRequest req = new LayoutUpdateRequest(project, dataflowId, layouts, segDetails);
        Preconditions.checkNotNull(jobType);
        req.setJobType(jobType.name());
        val jobId = StreamingUtils.getJobId(dataflowId, req.getJobType());
        req.setJobExecutionId(JobExecutionIdHolder.getJobExecutionId(jobId));
        try (RestSupport rest = new RestSupport(conf)) {
            rest.execute(rest.createHttpPut(url), req);
        }
        StreamingUtils.replayAuditlog();
    }
}

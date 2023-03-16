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

package org.apache.kylin.rest.config.initialize;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.springframework.stereotype.Component;

import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ModelUpdateListener {

    @Subscribe
    public void onModelRename(NDataModel.ModelRenameEvent event) {
        String project = event.getProject();
        String modelId = event.getSubject();
        String newName = event.getNewName();
        String buildId = String.format(Locale.ROOT, "%s_%s", modelId, "build");
        String mergeId = String.format(Locale.ROOT, "%s_%s", modelId, "merge");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    project);
            streamingJobManager.updateStreamingJob(buildId, copyForWrite -> copyForWrite.setModelName(newName));
            streamingJobManager.updateStreamingJob(mergeId, copyForWrite -> copyForWrite.setModelName(newName));
            return null;
        }, project);
    }
}

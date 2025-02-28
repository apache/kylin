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

package org.apache.kylin.engine.spark.job;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;

public class ResourceDetectBeforeOptimizeJob extends LayoutDataOptimizeJob implements ResourceDetect {

    public static void main(String[] args) {
        ResourceDetectBeforeOptimizeJob resourceDetectJob = new ResourceDetectBeforeOptimizeJob();
        resourceDetectJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        infos.clearOptimizeLayoutIds();
        Map<String, Long> resourceSize = Maps.newHashMap();
        this.getLayoutDetails().foreach(layoutDetails -> {
            infos.recordOptimizeLayoutIds(layoutDetails.getLayoutId());
            resourceSize.put(layoutDetails.getId(), layoutDetails.getSizeInBytes());
            return null;
        });
        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                dataFlow().getId() + "_" + ResourceDetectUtils.fileName()), resourceSize);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.resourceDetectBeforeOptimizeJob();
    }

    @Override
    protected void waitForResourceSuccess() {
        // do nothing
    }
}

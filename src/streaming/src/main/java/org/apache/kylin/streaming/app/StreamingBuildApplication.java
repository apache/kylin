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
package org.apache.kylin.streaming.app;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;

public abstract class StreamingBuildApplication extends StreamingApplication {

    //params
    protected int durationSec;
    protected String watermark;
    protected String baseCheckpointLocation;

    protected StreamingBuildApplication() {
        this.jobType = JobTypeEnum.STREAMING_BUILD;

        this.baseCheckpointLocation = kylinConfig.getStreamingBaseCheckpointLocation();
        Preconditions.checkState(StringUtils.isNotBlank(baseCheckpointLocation),
                "base checkpoint location must be configured, %s", baseCheckpointLocation);
    }

    public void parseParams(String[] args) {
        this.project = args[0];
        this.dataflowId = args[1];
        this.durationSec = Integer.parseInt(args[2]);
        this.watermark = args[3];
        this.distMetaUrl = args[4];
        this.jobId = StreamingUtils.getJobId(dataflowId, jobType.name());

        Preconditions.checkArgument(StringUtils.isNotEmpty(distMetaUrl), "distMetaUrl should not be empty!");
    }

}

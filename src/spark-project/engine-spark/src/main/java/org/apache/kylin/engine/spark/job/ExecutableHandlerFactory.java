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

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.job.execution.DefaultChainedExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableHandler;

import com.google.common.base.Preconditions;

import lombok.val;

public class ExecutableHandlerFactory {

    public static ExecutableHandler createExecutableHandler(DefaultChainedExecutableOnModel job) {
        val segmentIds = job.getTargetSegments();
        switch (job.getJobType()) {
        case INDEX_BUILD:
        case SUB_PARTITION_BUILD:
            return new ExecutableAddCuboidHandler(job);
        case INC_BUILD:
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(segmentIds));
            return new ExecutableAddSegmentHandler(job.getProject(), job.getTargetSubject(), job.getSubmitter(),
                    segmentIds.get(0), job.getId());
        case INDEX_MERGE:
        case INDEX_REFRESH:
        case SUB_PARTITION_REFRESH:
            Preconditions.checkArgument(CollectionUtils.isNotEmpty(segmentIds));
            return new ExecutableMergeOrRefreshHandler(job.getProject(), job.getTargetSubject(), job.getSubmitter(),
                    segmentIds.get(0), job.getId());
        default:
            throw new IllegalArgumentException("illegal job type");
        }
    }
}

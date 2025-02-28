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

import java.util.Collections;

import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.utils.GlutenCacheUtils;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InternalTableLoadCacheStep extends LoadCacheStep {

    // called by reflection
    public InternalTableLoadCacheStep() {
    }

    public InternalTableLoadCacheStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    public ExecuteResult doWork(JobContext context) throws ExecuteException {
        try (SetLogCategory ignore = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            val table = getParam(NBatchConstants.P_TABLE_NAME);
            val start = getParam(NBatchConstants.P_START_DATE);
            var cacheTableCommand = GlutenCacheUtils.generateCacheTableCommand(getConfig(), getProject(), table, start,
                    Collections.emptyList(), false);
            log.info("InternalTable[{}] cache command is [{}]", table, cacheTableCommand);
            routeCacheToAllQueryNode(getProject(), Sets.newHashSet(cacheTableCommand));
            return ExecuteResult.createSucceed();
        } catch (Throwable throwable) {
            log.warn("InternalTableLoadCache routeCacheToAllQueryNode failed.", throwable);
            return ExecuteResult.createError(throwable);
        }
    }
}

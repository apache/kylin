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

package org.apache.kylin.job.execution;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;

import lombok.val;

public class DefaultExecutableOnTable extends DefaultExecutable {

    public DefaultExecutableOnTable() {
        super();
    }

    public DefaultExecutableOnTable(Object notSetId) {
        super(notSetId);
    }

    public String getTableIdentity() {
        return getParam(NBatchConstants.P_TABLE_NAME);
    }

    @Override
    public String getTargetSubjectAlias() {
        return getTableIdentity();
    }

    @Override
    protected void afterUpdateOutput(String jobId) {
        val job = getExecutableManager(getProject()).getJob(jobId);
        long duration = job.getDuration();
        long endTime = job.getEndTime();
        long startOfDay = TimeUtil.getDayStart(endTime);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            JobStatisticsManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateStatistics(startOfDay,
                    duration, 0, 0);
            return true;
        }, project);
    }
}

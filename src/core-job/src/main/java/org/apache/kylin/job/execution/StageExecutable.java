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

import static org.apache.kylin.job.execution.JobTypeEnum.STAGE;

import org.apache.kylin.guava30.shaded.common.base.MoreObjects;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.exception.ExecuteException;

public class StageExecutable extends AbstractExecutable {

    public StageExecutable() {
    }

    public StageExecutable(Object notSetId) {
        super(notSetId);
    }

    public StageExecutable(String name) {
        this.setName(name);
        this.setJobType(STAGE);
    }

    @Override
    public ExecuteResult doWork(JobContext context) throws ExecuteException {
        return null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", getId()).add("name", getName()).add("state", getStatus())
                .toString();
    }

    public ExecutableState getStatus(String segmentId) {
        return getOutput(segmentId).getState();
    }

    public ExecutableState getStatusInMem(String segmentId) {
        return getOutput(segmentId, getPo()).getState();
    }

    public Output getOutput(String segmentId) {
        return getManager().getOutput(getId(), segmentId);
    }

    public Output getOutput(String segmentId, ExecutablePO executablePO) {
        return getManager().getOutput(getId(), executablePO, segmentId);
    }
}

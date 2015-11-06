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

package org.apache.kylin.engine.mr.invertedindex;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import java.io.IOException;

/**
 */
public class UpdateIIInfoAfterBuildStep extends AbstractExecutable {

    private static final String II_NAME = "iiName";
    private static final String JOB_ID = "jobId";

    public UpdateIIInfoAfterBuildStep() {
        super();
    }

    public void setInvertedIndexName(String cubeName) {
        this.setParam(II_NAME, cubeName);
    }

    private String getInvertedIndexName() {
        return getParam(II_NAME);
    }

    public void setJobId(String id) {
        setParam(JOB_ID, id);
    }

    private String getJobId() {
        return getParam(JOB_ID);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {

        IIManager mgr = IIManager.getInstance(KylinConfig.getInstanceFromEnv());
        IIInstance ii = mgr.getII(getInvertedIndexName());
        IISegment segment = ii.getFirstSegment();
        segment.setStatus(SegmentStatusEnum.READY);
        
        segment.setLastBuildJobID(getJobId());
        segment.setLastBuildTime(System.currentTimeMillis());

        try {
            mgr.updateII(ii);
            return new ExecuteResult(ExecuteResult.State.SUCCEED, "succeed");
        } catch (IOException e) {
            logger.error("fail to update inverted index after build", e);
            return new ExecuteResult(ExecuteResult.State.ERROR, e.getLocalizedMessage());
        }
    }
    
}

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

package org.apache.kylin.engine.mr;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.IBatchCubingEngine;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.TableSourceFactory;

public class MRBatchCubingEngine implements IBatchCubingEngine {

    @Override
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter) {
        return new BatchCubingJobBuilder(newSegment, submitter).build();
    }

    @Override
    public DefaultChainedExecutable createBatchMergeJob(CubeSegment mergeSegment, String submitter) {
        return new BatchMergeJobBuilder(mergeSegment, submitter).build();
    }
    
    public static IMRBatchCubingInputSide getBatchCubingInputSide(CubeSegment seg) {
        TableDesc tableDesc = getTableDesc(seg.getCubeDesc().getFactTable());
        return getMRInput(tableDesc).getBatchCubingInputSide(seg);
    }

    public static IMRTableInputFormat getTableInputFormat(String tableName) {
        return getTableInputFormat(getTableDesc(tableName));
    }

    public static IMRTableInputFormat getTableInputFormat(TableDesc tableDesc) {
        return getMRInput(tableDesc).getTableInputFormat(tableDesc);
    }

    private static IMRInput getMRInput(TableDesc tableDesc) {
        return TableSourceFactory.createEngineAdapter(tableDesc, IMRInput.class);
    }

    private static TableDesc getTableDesc(String tableName) {
        return MetadataManager.getInstance(KylinConfig.getInstanceFromEnv()).getTableDesc(tableName);
    }

}

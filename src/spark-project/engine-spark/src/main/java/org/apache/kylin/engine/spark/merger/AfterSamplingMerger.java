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

package org.apache.kylin.engine.spark.merger;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;

import lombok.val;

public class AfterSamplingMerger extends MetadataMerger {
    
    public AfterSamplingMerger(KylinConfig config, String project) {
        super(config, project);
    }

    private void mergeRemoteMetaAfterSampling(String outputMetaUrl, String tableIdentity, long createTime) {
        String project = getProject();
        try (val remoteStore = ExecutableUtils.getRemoteStore(KylinConfig.getInstanceFromEnv(), outputMetaUrl)) {
            val remoteTblMgr = NTableMetadataManager.getInstance(remoteStore.getConfig(), project);
            val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            localTblMgr.mergeAndUpdateTableExt(localTblMgr.getOrCreateTableExt(tableIdentity),
                    remoteTblMgr.getOrCreateTableExt(tableIdentity));

            // use create time of sampling job to update the create time of TableExtDesc
            final TableDesc tableDesc = localTblMgr.getTableDesc(tableIdentity);
            final TableExtDesc tableExt = localTblMgr.getTableExtIfExists(tableDesc);
            TableExtDesc copyForWrite = localTblMgr.copyForWrite(tableExt);
            copyForWrite.setCreateTime(createTime);
            localTblMgr.saveTableExt(copyForWrite);
        }
    }

    @Override
    public <T> T merge(MergerInfo.TaskMergeInfo info) {
        mergeRemoteMetaAfterSampling(info.getOutputMetaUrl(), info.getTableIdentity(), info.getCreateTime());
        return null;
    }
}

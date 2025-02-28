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

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AfterLoadingInternalTableMerger extends MetadataMerger {

    public AfterLoadingInternalTableMerger(KylinConfig config, String project) {
        super(config, project);
    }

    public void mergeRemoteMetaAfterLoadingInternalTable(String outputMetaUrl, String tableIdentity) {
        try (val remoteStore = ExecutableUtils.getRemoteStore(KylinConfig.getInstanceFromEnv(), outputMetaUrl)) {
            val project = getProject();
            val remoteInternalTblMgr = InternalTableManager.getInstance(remoteStore.getConfig(), project);
            val remoteInternalTbDesc = remoteInternalTblMgr.getInternalTableDesc(tableIdentity);

            val fs = HadoopUtil.getWorkingFileSystem();

            long storageSize = 0;
            try {
                storageSize = HadoopUtil.getContentSummary(fs, new Path(remoteInternalTbDesc.getLocation()))
                        .getLength();
            } catch (IOException e) {
                log.warn("Fetch internal table size for {} from {} failed", remoteInternalTbDesc.getIdentity(),
                        remoteInternalTbDesc.getLocation());
            }
            remoteInternalTbDesc.setStorageSize(storageSize);
            mergeRemoteMetaAfterLoadingInternalTable(remoteInternalTbDesc, tableIdentity);
        }
    }

    private void mergeRemoteMetaAfterLoadingInternalTable(InternalTableDesc remoteInternalTableDesc,
            String tableIdentity) {
        val project = getProject();
        val localInternalTblMgr = InternalTableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val localInternalTbDesc = localInternalTblMgr.getInternalTableDesc(tableIdentity);
        val copy = localInternalTblMgr.copyForWrite(localInternalTbDesc);
        copy.setStorageSize(remoteInternalTableDesc.getStorageSize());
        copy.setRowCount(remoteInternalTableDesc.getRowCount());
        copy.setTablePartition(remoteInternalTableDesc.getTablePartition());
        localInternalTblMgr.saveOrUpdateInternalTable(copy);
    }

    @Override
    public <T> T merge(MergerInfo.TaskMergeInfo info) {
        mergeRemoteMetaAfterLoadingInternalTable(info.getOutputMetaUrl(), info.getTableIdentity());
        return null;
    }
}

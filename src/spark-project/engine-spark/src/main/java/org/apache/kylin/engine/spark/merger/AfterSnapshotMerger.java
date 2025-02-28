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
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.engine.spark.job.SnapshotBuildFinishedEvent;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AfterSnapshotMerger extends MetadataMerger {

    public AfterSnapshotMerger(KylinConfig config, String project) {
        super(config, project);
    }

    public void mergeRemoteMetaAfterSnapshot(String outputMetaUrl, String tableIdentity, String selectPartCol,
            boolean incrementBuild) {
        try (val remoteStore = ExecutableUtils.getRemoteStore(KylinConfig.getInstanceFromEnv(), outputMetaUrl)) {
            val project = getProject();
            val remoteTblMgr = NTableMetadataManager.getInstance(remoteStore.getConfig(), project);
            val remoteTbDesc = remoteTblMgr.getTableDesc(tableIdentity);
            val remoteTblExtDesc = remoteTblMgr.getOrCreateTableExt(remoteTbDesc);
            val fs = HadoopUtil.getWorkingFileSystem();
            val baseDir = KapConfig.getInstanceFromEnv().getMetadataWorkingDirectory();

            if (selectPartCol != null && !incrementBuild) {
                remoteTbDesc.setLastSnapshotPath(remoteTbDesc.getTempSnapshotPath());
            }
            long snapshotSize = 0;
            try {
                snapshotSize = HadoopUtil.getContentSummary(fs, new Path(baseDir + remoteTbDesc.getLastSnapshotPath()))
                        .getLength();
            } catch (IOException e) {
                log.warn("Fetch snapshot size for {} from {} failed", remoteTbDesc.getIdentity(),
                        baseDir + remoteTbDesc.getLastSnapshotPath());
            }
            remoteTbDesc.setLastSnapshotSize(snapshotSize);
            EventBusFactory.getInstance()
                    .postSync(new SnapshotBuildFinishedEvent(remoteTbDesc, selectPartCol, incrementBuild));
            mergeRemoteMetaAfterBuilding(remoteTbDesc, remoteTblExtDesc, tableIdentity, selectPartCol);
        }
    }

    private void mergeRemoteMetaAfterBuilding(TableDesc remoteTbDesc, TableExtDesc remoteTblExtDesc,
            String tableIdentity, String selectPartCol) {

        val project = getProject();
        val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val localTbDesc = localTblMgr.getTableDesc(tableIdentity);
        val copy = localTblMgr.copyForWrite(localTbDesc);
        val copyExt = localTblMgr.copyForWrite(localTblMgr.getOrCreateTableExt(localTbDesc));

        copy.setLastSnapshotPath(remoteTbDesc.getLastSnapshotPath());
        copy.setLastSnapshotSize(remoteTbDesc.getLastSnapshotSize());
        copy.setSnapshotLastModified(System.currentTimeMillis());
        copy.setSnapshotHasBroken(false);
        copy.setSnapshotTotalRows(remoteTbDesc.getSnapshotTotalRows());
        if (selectPartCol == null) {
            copyExt.setOriginalSize(remoteTblExtDesc.getOriginalSize());
            copy.setSnapshotPartitionCol(null);
            copy.resetSnapshotPartitions(Sets.newHashSet());
        } else {
            copyExt.setOriginalSize(remoteTbDesc.getSnapshotPartitions().values().stream().mapToLong(i -> i).sum());
            copy.setSnapshotPartitionCol(selectPartCol);
            copy.setSnapshotPartitions(remoteTbDesc.getSnapshotPartitions());
            copy.setSnapshotPartitionsInfo(remoteTbDesc.getSnapshotPartitionsInfo());
        }

        copyExt.setTotalRows(remoteTblExtDesc.getTotalRows());
        localTblMgr.saveTableExt(copyExt);
        localTblMgr.updateTableDesc(copy);
    }

    @Override
    public <T> T merge(MergerInfo.TaskMergeInfo info) {
        mergeRemoteMetaAfterSnapshot(info.getOutputMetaUrl(), info.getTableIdentity(), info.getSelectPartCol(),
                info.isIncrementBuild());
        return null;
    }
}

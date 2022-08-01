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

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.model.SnapshotBuildFinishedEvent;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import lombok.NoArgsConstructor;
import lombok.val;

/**
 */
@NoArgsConstructor
public class NSparkSnapshotBuildingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkSnapshotBuildingStep.class);

    public NSparkSnapshotBuildingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_SNAPSHOT);
    }

    public NSparkSnapshotBuildingStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        final Set<String> dumpList = Sets.newHashSet();
        final String table = getParam(NBatchConstants.P_TABLE_NAME);
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(config, getProject());
        final TableDesc tableDesc = tblMgr.getTableDesc(table);
        final ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(this.getProject());
        final TableExtDesc tableExtDesc = tblMgr.getTableExtIfExists(tableDesc);
        if (tableExtDesc != null) {
            dumpList.add(tableExtDesc.getResourcePath());
        }
        dumpList.add(tableDesc.getResourcePath());
        dumpList.add(projectInstance.getResourcePath());

        return dumpList;
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        ExecuteResult result = super.doWork(context);
        if (!result.succeed()) {
            return result;
        }

        try (val remoteStore = ExecutableUtils.getRemoteStore(KylinConfig.getInstanceFromEnv(), this)) {
            String tableName = getParam(NBatchConstants.P_TABLE_NAME);
            String selectPartCol = getParam(NBatchConstants.P_SELECTED_PARTITION_COL);
            boolean incrementBuild = "true".equals(getParam(NBatchConstants.P_INCREMENTAL_BUILD));

            val remoteTblMgr = NTableMetadataManager.getInstance(remoteStore.getConfig(), getProject());
            val remoteTbDesc = remoteTblMgr.getTableDesc(tableName);
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
                logger.warn("Fetch snapshot size for {} from {} failed", remoteTbDesc.getIdentity(),
                        baseDir + remoteTbDesc.getLastSnapshotPath());
            }
            remoteTbDesc.setLastSnapshotSize(snapshotSize);
            EventBusFactory.getInstance()
                    .postSync(new SnapshotBuildFinishedEvent(remoteTbDesc, selectPartCol, incrementBuild));
            wrapWithCheckQuit(() -> mergeRemoteMetaAfterBuilding(remoteTbDesc, remoteTblExtDesc));
        }
        return result;
    }

    private void mergeRemoteMetaAfterBuilding(TableDesc remoteTbDesc, TableExtDesc remoteTblExtDesc) {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        String selectPartCol = getParam(NBatchConstants.P_SELECTED_PARTITION_COL);

        val localTblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
        val localTbDesc = localTblMgr.getTableDesc(tableName);
        val copy = localTblMgr.copyForWrite(localTbDesc);
        val copyExt = localTblMgr.copyForWrite(localTblMgr.getOrCreateTableExt(localTbDesc));

        copy.setLastSnapshotPath(remoteTbDesc.getLastSnapshotPath());
        copy.setLastSnapshotSize(remoteTbDesc.getLastSnapshotSize());
        copy.setSnapshotLastModified(System.currentTimeMillis());
        copy.setSnapshotHasBroken(false);
        if (selectPartCol == null) {
            copyExt.setOriginalSize(remoteTblExtDesc.getOriginalSize());
            copy.setSnapshotPartitionCol(null);
            copy.resetSnapshotPartitions(Sets.newHashSet());
            copy.setSnapshotTotalRows(remoteTbDesc.getSnapshotTotalRows());
        } else {
            copyExt.setOriginalSize(remoteTbDesc.getSnapshotPartitions().values().stream().mapToLong(i -> i).sum());
            copy.setSnapshotPartitionCol(selectPartCol);
            copy.setSnapshotPartitions(remoteTbDesc.getSnapshotPartitions());
            copy.setSnapshotPartitionsInfo(remoteTbDesc.getSnapshotPartitionsInfo());
            copy.setSnapshotTotalRows(remoteTbDesc.getSnapshotTotalRows());
        }

        copyExt.setTotalRows(remoteTblExtDesc.getTotalRows());
        localTblMgr.saveTableExt(copyExt);
        localTblMgr.updateTableDesc(copy);

    }

    public static class Mockup {
        public static void main(String[] args) {
            String msg = String.format(Locale.ROOT, "%s.main() invoked, args: %s", Mockup.class, Arrays.toString(args));
            logger.info(msg);
        }
    }
}

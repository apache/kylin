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

import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.ExecutableUtils;
import org.apache.kylin.engine.spark.cleanup.SnapshotChecker;
import org.apache.kylin.engine.spark.utils.FileNames;
import org.apache.kylin.engine.spark.utils.HDFSUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class NSparkUpdateMetadataStep extends AbstractExecutable {

    public NSparkUpdateMetadataStep() {
        this.setName(ExecutableConstants.STEP_UPDATE_METADATA);
    }

    public NSparkUpdateMetadataStep(Object notSetId) {
        super(notSetId);
    }

    private static final Logger logger = LoggerFactory.getLogger(NSparkUpdateMetadataStep.class);

    @Override
    protected ExecuteResult doWork(JobContext context) throws ExecuteException {
        val parent = getParent();
        Preconditions.checkArgument(parent instanceof DefaultExecutableOnModel);
        val handler = ((DefaultExecutableOnModel) parent).getHandler();
        try {
            handler.handleFinished();
            cleanExpiredSnapshot();
            return ExecuteResult.createSucceed();
        } catch (Throwable throwable) {
            logger.warn("update metadata failed.", throwable);
            return ExecuteResult.createError(throwable);
        }
    }

    private void cleanExpiredSnapshot() {
        try {
            long startDelete = System.currentTimeMillis();
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            String workingDir = KapConfig.wrap(config).getMetadataWorkingDirectory();
            long survivalTimeThreshold = config.getSnapshotVersionTTL();
            String dfId = ExecutableUtils.getDataflowId(this);
            NDataflow dataflow = NDataflowManager.getInstance(config, getProject()).getDataflow(dfId);
            Set<TableRef> tables = dataflow.getModel().getLookupTables();
            for (TableRef table : tables) {
                if (table.getTableDesc().getLastSnapshotPath() == null) {
                    continue;
                }

                Path path = FileNames.snapshotFileWithWorkingDir(project, table.getTableIdentity(), workingDir);
                if (!HDFSUtils.exists(path) && config.isUTEnv()) {
                    continue;
                }
                FileStatus lastFile = HDFSUtils.findLastFile(path);
                HDFSUtils.deleteFilesWithCheck(path, new SnapshotChecker(config.getSnapshotMaxVersions(),
                        survivalTimeThreshold, lastFile.getModificationTime()));
            }
            logger.info("Delete expired snapshot table for dataflow {} cost: {} ms.", dfId,
                    (System.currentTimeMillis() - startDelete));
        } catch (Exception e) {
            logger.error("error happen in cleaning expired snapshot ", e);
        }
    }
}

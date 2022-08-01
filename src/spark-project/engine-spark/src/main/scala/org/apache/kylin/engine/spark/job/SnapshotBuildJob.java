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

import static org.apache.kylin.engine.spark.job.StageType.SNAPSHOT_BUILD;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.StringSplitter;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.SnapshotBuilder;
import org.apache.kylin.engine.spark.builder.SnapshotPartitionBuilder;
import org.apache.kylin.engine.spark.job.exec.SnapshotExec;
import org.apache.kylin.engine.spark.utils.FileNames;
import org.apache.kylin.engine.spark.utils.SparkConfHelper;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import lombok.val;

public class SnapshotBuildJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(SnapshotBuildJob.class);

    private static Set<String> toPartitions(String tableListStr) {
        if (StringUtils.isBlank(tableListStr)) {
            return null;
        }
        return ImmutableSet.<String> builder().addAll(Arrays.asList(StringSplitter.split(tableListStr, ","))).build();
    }

    public static void main(String[] args) {
        SnapshotBuildJob snapshotBuildJob = new SnapshotBuildJob();
        snapshotBuildJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
        val exec = new SnapshotExec(jobStepId);

        SNAPSHOT_BUILD.createStage(this, null, null, exec);
        exec.buildSnapshot();
    }

    public void buildSnapshot() throws IOException {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        String selectedPartCol = getParam(NBatchConstants.P_SELECTED_PARTITION_COL);
        TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
        boolean incrementalBuild = "true".equals(getParam(NBatchConstants.P_INCREMENTAL_BUILD));
        String partitionToBuildString = getParam(NBatchConstants.P_SELECTED_PARTITION_VALUE);
        Set<String> partitionToBuild = null;
        if (partitionToBuildString != null) {
            partitionToBuild = JsonUtil.readValueAsSet(partitionToBuildString);
        }

        if (selectedPartCol == null) {
            new SnapshotBuilder().buildSnapshot(ss, Sets.newHashSet(tableDesc));
        } else {
            initialize(tableDesc, selectedPartCol, incrementalBuild, partitionToBuild);

            tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
            if (partitionToBuild == null) {
                partitionToBuild = tableDesc.getNotReadyPartitions();
            }
            logger.info("{} need build partitions: {}", tableDesc.getIdentity(), partitionToBuild);

            new SnapshotPartitionBuilder().buildSnapshot(ss, tableDesc, selectedPartCol, partitionToBuild);

            if (incrementalBuild) {
                moveIncrementalPartitions(tableDesc.getLastSnapshotPath(), tableDesc.getTempSnapshotPath());
            }
        }
    }

    private void initialize(TableDesc table, String selectedPartCol, boolean incrementBuild,
            Set<String> partitionToBuild) {
        if (table.getTempSnapshotPath() != null) {
            logger.info("snapshot partition has been initialed, so skip.");
            return;
        }
        Set<String> partitions = getTablePartitions(table, selectedPartCol);
        Set<String> curPartitions = table.getSnapshotPartitions().keySet();
        String resourcePath = FileNames.snapshotFile(table) + "/" + RandomUtil.randomUUID();

        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tableMetadataManager = NTableMetadataManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project);
            TableDesc copy = tableMetadataManager.copyForWrite(table);
            if (incrementBuild) {
                if (partitionToBuild == null) {
                    copy.addSnapshotPartitions(Sets.difference(partitions, curPartitions));
                } else {
                    copy.addSnapshotPartitions(partitionToBuild);
                }
            } else {
                copy.resetSnapshotPartitions(partitions);
                copy.setSnapshotTotalRows(0);
                TableExtDesc copyExt = tableMetadataManager
                        .copyForWrite(tableMetadataManager.getOrCreateTableExt(table));
                copyExt.setTotalRows(0);
                tableMetadataManager.saveTableExt(copyExt);
            }
            copy.setTempSnapshotPath(resourcePath);
            tableMetadataManager.updateTableDesc(copy);
            return null;
        }, project);

    }

    protected Set<String> getTablePartitions(TableDesc tableDesc, String selectPartitionCol) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return toPartitions(getParam("partitions"));
        }

        if (tableDesc.isRangePartition() && tableDesc.getPartitionColumn().equalsIgnoreCase(selectPartitionCol)) {
            logger.info("The【{}】column is range partition table,so return partition column.", tableDesc.getName());
            return toPartitions(tableDesc.getPartitionColumn());
        }

        ISourceMetadataExplorer explr = SourceFactory.getSource(tableDesc).getSourceMetadataExplorer();
        Set<String> curPartitions = explr.getTablePartitions(tableDesc.getDatabase(), tableDesc.getName(),
                tableDesc.getProject(), selectPartitionCol);

        logger.info("{} current partitions: {}", tableDesc.getIdentity(), curPartitions);
        return curPartitions;
    }

    private void moveIncrementalPartitions(String originSnapshotPath, String incrementalSnapshotPath) {
        String target = getSnapshotDir(originSnapshotPath);
        Path sourcePath = new Path(getSnapshotDir(incrementalSnapshotPath));
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        try {
            if (!fs.exists(sourcePath)) {
                return;
            }
            for (FileStatus fileStatus : fs.listStatus(sourcePath)) {
                final String targetFilePathString = target + "/" + fileStatus.getPath().getName();
                Path targetFilePath = new Path(targetFilePathString);
                if (fs.exists(targetFilePath)) {
                    logger.info(String.format(Locale.ROOT, "delete non-effective partition %s ", targetFilePath));
                    fs.delete(targetFilePath, true);
                }
                if (StringUtils.equalsIgnoreCase(fs.getScheme(), "s3a") && fs.isDirectory(fileStatus.getPath())) {
                    fs.mkdirs(targetFilePath);
                    renameS3A(fs, fileStatus, targetFilePath);
                } else {
                    fs.rename(fileStatus.getPath(), new Path(target));
                }
            }

            fs.delete(sourcePath, true);
        } catch (Exception e) {
            logger.error(String.format(Locale.ROOT, "from %s to %s move file fail:", incrementalSnapshotPath,
                    originSnapshotPath), e);
            Throwables.propagate(e);
        }

    }

    private void renameS3A(FileSystem fs, FileStatus source, Path target) throws IOException {
        for (FileStatus sourceInner : fs.listStatus(source.getPath())) {
            if (!fs.exists(sourceInner.getPath())) {
                continue;
            }
            if (sourceInner.isFile()) {
                fs.rename(sourceInner.getPath(), target);
            }
            if (sourceInner.isDirectory()) {
                final String targetInnerString = target + "/" + sourceInner.getPath().getName();
                Path targetInner = new Path(targetInnerString);
                if (fs.exists(targetInner)) {
                    logger.info(String.format(Locale.ROOT, "delete non-effective partition %s ", targetInnerString));
                    fs.delete(targetInner, true);
                }
                fs.mkdirs(targetInner);
                renameS3A(fs, sourceInner, targetInner);
            }
        }
    }

    private String getSnapshotDir(String snapshotPath) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String workingDir = KapConfig.wrap(config).getMetadataWorkingDirectory();
        return workingDir + "/" + snapshotPath;
    }

    @Override
    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> snapshotConfig = config.getSnapshotBuildingConfigOverride();
        Map<String, String> generalBuildConfig = config.getSparkConfigOverride();
        generalBuildConfig.putAll(snapshotConfig);
        return generalBuildConfig;
    }

    @Override
    protected void chooseContentSize(SparkConfHelper helper) {
        return;
    }

}

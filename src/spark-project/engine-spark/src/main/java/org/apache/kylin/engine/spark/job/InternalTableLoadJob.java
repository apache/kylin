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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.engine.spark.builder.InternalTableLoader;
import org.apache.kylin.engine.spark.job.exec.InternalTableLoadExec;
import org.apache.kylin.engine.spark.job.stage.internal.InternalTableLoad;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.metadata.table.InternalTablePartition;
import org.apache.kylin.metadata.table.InternalTablePartitionDetail;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.tables.ClickhouseTable;
import io.delta.tables.DeltaTable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.val;

//
public class InternalTableLoadJob extends SparkApplication {
    protected static final Logger logger = LoggerFactory.getLogger(InternalTableLoadJob.class);

    public static void main(String[] args) {
        InternalTableLoadJob job = new InternalTableLoadJob();
        job.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
        val exec = new InternalTableLoadExec(jobStepId);
        exec.addStage(new InternalTableLoad(this));
        exec.executeStep();
    }

    public void innerExecute() throws IOException {
        boolean dropPartition = Boolean.parseBoolean(getParam(NBatchConstants.P_DELETE_PARTITION));
        if (dropPartition) {
            logger.info("Start to drop partitions");
            dropPartitons();
        } else {
            logger.info("Start to load data into table");
            loadIntoInternalTable();
        }
        updateMate();
    }

    private void updateMate() {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        InternalTableMetaUpdateInfo info = extractUpdateInfo(project, tableName, config, ss);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            InternalTableManager internalTableManager = InternalTableManager.getInstance(config, project);
            InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(tableName);
            InternalTablePartition tablePartition = internalTable.getTablePartition();
            if (tablePartition != null) {
                tablePartition.setPartitionValues(info.getPartitionValues());
                tablePartition.setPartitionDetails(info.getPartitionDetails());
            }
            internalTable.setRowCount(info.getFinalCount());
            internalTableManager.saveOrUpdateInternalTable(internalTable);
            return true;
        }, project);
    }

    public InternalTableMetaUpdateInfo extractUpdateInfo(String project, String tableName, KylinConfig config,
            SparkSession ss) {
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, project);
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(tableName);
        List<InternalTablePartitionDetail> partitionDetails;
        long count = getInternalTableCount(internalTable, ss);
        if (internalTable.getTablePartition() != null) {
            partitionDetails = extractPartitionDetails(ss, internalTable);
        } else {
            partitionDetails = Collections.emptyList();
        }
        if (!partitionDetails.isEmpty()) {
            partitionDetails.sort(Comparator.comparing(InternalTablePartitionDetail::getPartitionValue));
        }
        logger.info("update internal table meta data : row count:{}, partition size:{}", count,
                partitionDetails.size());
        List<String> finalPartitionValues = partitionDetails.stream()
                .map(InternalTablePartitionDetail::getPartitionValue).collect(Collectors.toList());
        return new InternalTableMetaUpdateInfo(count, finalPartitionValues, partitionDetails);
    }

    @VisibleForTesting
    public long getInternalTableCount(InternalTableDesc internalTable, SparkSession ss) {
        switch (internalTable.getStorageType()) {
        case PARQUET:
            return ss.read().format(internalTable.getStorageType().getFormat()).load(internalTable.getLocation())
                    .count();
        case GLUTEN:
            return ClickhouseTable.forPath(ss, internalTable.getLocation()).toDF().count();
        case DELTALAKE:
            return DeltaTable.forPath(ss, internalTable.getLocation()).toDF().count();
        default:
            return 0;
        }
    }

    private List<InternalTablePartitionDetail> extractPartitionDetails(SparkSession ss,
            InternalTableDesc internalTable) {
        val partitionCol = internalTable.getTablePartition().getPartitionColumns()[0];
        List<String> partitionValues;
        List<InternalTablePartitionDetail> partitionDetails = new ArrayList<>();
        if (internalTable.getStorageType() == InternalTableDesc.StorageType.PARQUET) {
            try {
                val internalTableDs = ss.read().format(internalTable.getStorageType().getFormat())
                        .load(internalTable.getLocation());
                val partitionInfo = internalTableDs.select(partitionCol).distinct();
                partitionValues = partitionInfo.collectAsList().stream().map(row -> row.get(0)).map(Object::toString)
                        .collect(Collectors.toList());
                val fs = HadoopUtil.getWorkingFileSystem();
                for (String partitionValue : partitionValues) {
                    val subPath = partitionCol + "=" + partitionValue;
                    val partitionsPath = new Path(internalTable.getLocation(), subPath);
                    long storageSize = getStorageSizeWithoutException(fs, partitionsPath, subPath);
                    InternalTablePartitionDetail detail = new InternalTablePartitionDetail();
                    detail.setSizeInBytes(storageSize);
                    detail.setStoragePath(partitionsPath.toString());
                    detail.setPartitionValue(partitionValue);
                    partitionDetails.add(detail);
                }
            } catch (Exception e) {
                logger.warn("Can not get parquet info from internal table path {} caused by:",
                        internalTable.getLocation(), e);
            }
        } else {
            partitionValues = new ArrayList<>();
            InternalTableLoader loader = new InternalTableLoader();
            val partitionInfos = loader.getPartitionInfos(ss, internalTable);
            for (Row row : partitionInfos) {
                InternalTablePartitionDetail detail = new InternalTablePartitionDetail();
                String partitionValue = row.getString(0);
                long partitionStorageSize = row.getLong(1);
                long fileSize = row.getLong(2);
                val subPath = partitionCol + "=" + partitionValue;
                val partitionsPath = new Path(internalTable.getLocation(), subPath);
                partitionValues.add(partitionValue);
                detail.setSizeInBytes(partitionStorageSize);
                detail.setStoragePath(partitionsPath.toString());
                detail.setFileCount(fileSize);
                detail.setPartitionValue(partitionValue);
                partitionDetails.add(detail);
            }
        }
        return partitionDetails;
    }

    private long getStorageSizeWithoutException(FileSystem fs, Path partitionsPath, String subPath) {
        try {
            return HadoopUtil.getContentSummary(fs, partitionsPath).getLength();
        } catch (IOException e) {
            logger.warn("failed to get size of path {}", subPath, e);
            return -1;
        }
    }

    public void loadIntoInternalTable() throws IOException {
        String tableIdentity = getParam(NBatchConstants.P_TABLE_NAME);
        InternalTableDesc internalTable = InternalTableManager.getInstance(config, project)
                .getInternalTableDesc(tableIdentity);
        boolean incrementalBuild = "true".equals(getParam(NBatchConstants.P_INCREMENTAL_BUILD));
        String outPutMode = getParam(NBatchConstants.P_OUTPUT_MODE);
        String startDate = getParam(NBatchConstants.P_START_DATE);
        String endDate = getParam(NBatchConstants.P_END_DATE);
        String storagePolicy = config.getGlutenStoragePolicy();
        InternalTableLoader loader = new InternalTableLoader();
        loader.loadInternalTable(ss, internalTable, outPutMode, startDate, endDate, storagePolicy, incrementalBuild);
    }

    public void dropPartitons() throws IOException {
        InternalTableLoader loader = new InternalTableLoader();
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        String toBeDelete = getParam(NBatchConstants.P_DELETE_PARTITION_VALUES);
        InternalTableDesc internalTable = InternalTableManager.getInstance(config, project)
                .getInternalTableDesc(tableName);
        loader.dropPartitions(ss, internalTable, toBeDelete);
    }

    @Getter
    @AllArgsConstructor
    public static class InternalTableMetaUpdateInfo {
        long finalCount;
        List<String> partitionValues;
        List<InternalTablePartitionDetail> partitionDetails;
    }
}

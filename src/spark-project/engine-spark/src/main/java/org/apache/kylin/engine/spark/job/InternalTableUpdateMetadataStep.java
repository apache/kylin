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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.builder.InternalTableLoader;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnTable;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.metadata.table.InternalTablePartition;
import org.apache.kylin.metadata.table.InternalTablePartitionDetail;
import org.apache.kylin.util.DataRangeUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.tables.ClickhouseTable;
import io.delta.tables.DeltaTable;
import lombok.val;

public class InternalTableUpdateMetadataStep extends AbstractExecutable {

    public InternalTableUpdateMetadataStep() {
        this.setName(ExecutableConstants.STEP_UPDATE_METADATA);
    }

    public InternalTableUpdateMetadataStep(Object notSetId) {
        super(notSetId);
    }

    private static final Logger logger = LoggerFactory.getLogger(InternalTableUpdateMetadataStep.class);

    @Override
    protected ExecuteResult doWork(JobContext context) throws ExecuteException {
        val parent = getParent();
        Preconditions.checkArgument(parent instanceof DefaultExecutableOnTable);
        try {
            updateInternalTableMetadata();
            return ExecuteResult.createSucceed();
        } catch (Throwable throwable) {
            logger.warn("update internal table metadata failed.", throwable);
            return ExecuteResult.createError(throwable);
        }
    }

    private void updateInternalTableMetadata() {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            long startTime = System.currentTimeMillis();
            KylinConfig config = getConfig();
            SparkSession ss = SparkSession.getDefaultSession().get();
            String tableName = getParam(NBatchConstants.P_TABLE_NAME);
            String project = getParam(NBatchConstants.P_PROJECT_NAME);
            String startDate = getParam(NBatchConstants.P_START_DATE);
            String endDate = getParam(NBatchConstants.P_END_DATE);
            // fetch delta partition info
            InternalTableManager internalTableManager = InternalTableManager.getInstance(config, project);
            InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(tableName);
            extractPartitionDetails(ss, internalTable);
            InternalTablePartition tablePartition = internalTable.getTablePartition();
            InternalTableLoadJob.InternalTableMetaUpdateInfo info = extractUpdateInfo(project, tableName, config, ss);
            if (tablePartition != null) {
                tablePartition.setPartitionValues(info.getPartitionValues());
                tablePartition.setPartitionDetails(info.getPartitionDetails());
            }
            internalTable.setRowCount(info.getFinalCount());
            logger.info("starting merging delta partitions");
            if (null != tablePartition && StringUtils.isNotEmpty(tablePartition.getDatePartitionFormat())) {
                List<String[]> partitionRange = DataRangeUtils.mergeTimeRange(tablePartition.getPartitionValues(),
                        tablePartition.getDatePartitionFormat());
                internalTable.setPartitionRange(partitionRange);
            }
            // release current job_range
            String[] curJobRange = new String[] { "0", "0" };
            if (null != tablePartition && StringUtils.isNotEmpty(tablePartition.getDatePartitionFormat())
                    && StringUtils.isNotEmpty(startDate)) {
                SimpleDateFormat fmt = new SimpleDateFormat(tablePartition.getDatePartitionFormat(), Locale.ROOT);
                curJobRange = new String[] { fmt.format(Long.parseLong(startDate)),
                        fmt.format(Long.parseLong(endDate)) };
            }
            List<String[]> jobRange = internalTable.getJobRange();
            String[] finalCurJobRange = curJobRange;
            jobRange.removeIf(rang -> rang[0].equals(finalCurJobRange[0]) && rang[1].equals(finalCurJobRange[1]));
            internalTable.setJobRange(jobRange);
            logger.info("trying to release job_range for internal table {} , range {}.",
                    internalTable.getTableDesc().getTableAlias(), jobRange);

            internalTableManager.saveOrUpdateInternalTable(internalTable);
            logger.info("update metadata for internal table {} cost: {} ms.",
                    internalTable.getTableDesc().getTableAlias(), (System.currentTimeMillis() - startTime));
            return true;
        }, project);
    }

    public InternalTableLoadJob.InternalTableMetaUpdateInfo extractUpdateInfo(String project, String tableName,
            KylinConfig config, SparkSession ss) {
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
        return new InternalTableLoadJob.InternalTableMetaUpdateInfo(count, finalPartitionValues, partitionDetails);
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

    public List<InternalTablePartitionDetail> extractPartitionDetails(SparkSession ss,
            InternalTableDesc internalTable) {
        long startTime = System.currentTimeMillis();
        if (Objects.isNull(internalTable.getTablePartition())) {
            return Collections.emptyList();
        }
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
        logger.info("[ UPDATE_INTERNAL_TABLE] extract partitions from delta table cost {} ms",
                System.currentTimeMillis() - startTime);
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

}

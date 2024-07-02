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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.INTERNAL_TABLE_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.INTERNAL_TABLE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.INTERNAL_TABLE_PARTITION_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_INTERNAL_TABLE_PARAMETER;
import static org.apache.kylin.job.execution.JobTypeEnum.INTERNAL_TABLE_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.INTERNAL_TABLE_REFRESH;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.engine.spark.builder.InternalTableLoader;
import org.apache.kylin.engine.spark.job.InternalTableLoadJob;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.metadata.table.InternalTablePartition;
import org.apache.kylin.metadata.table.InternalTablePartitionDetail;
import org.apache.kylin.rest.response.InternalTableLoadingJobResponse;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

@Service("internalTableLoadingService")
public class InternalTableLoadingService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(InternalTableLoadingService.class);

    public InternalTableLoadingJobResponse loadIntoInternalTable(String project, String table, String database,
            boolean isIncremental, boolean isRefresh, String startDate, String endDate, String yarnQueue) {
        JobManager.checkStorageQuota(project);
        List<String> jobIds = new ArrayList<>();
        JobTypeEnum jobType = isRefresh ? INTERNAL_TABLE_REFRESH : INTERNAL_TABLE_BUILD;
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            JobManager.checkStorageQuota(project);
            JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getConfig(), project);
            jobStatisticsManager.updateStatistics(TimeUtil.getDayStart(System.currentTimeMillis()), 0, 0, 1);
            InternalTableDesc internalTable = checkAndGetInternalTables(project, table, database);
            if (isIncremental && (Objects.isNull(internalTable.getTablePartition())
                    || Objects.isNull(internalTable.getTablePartition().getPartitionColumns())
                    || internalTable.getTablePartition().getPartitionColumns().length == 0)) {
                String errorMsg = String.format(Locale.ROOT, MsgPicker.getMsg().getInternalTableUnpartitioned());
                throw new KylinException(INTERNAL_TABLE_ERROR, errorMsg);
            }
            // check refresh time exceed loaded range
            InternalTablePartition tablePartition = internalTable.getTablePartition();
            if (jobType == INTERNAL_TABLE_REFRESH && !Objects.isNull(tablePartition)
                    && StringUtils.isNotEmpty(tablePartition.getDatePartitionFormat())
                    && !CollectionUtils.isEmpty(tablePartition.getPartitionValues())) {
                // List is sorted in ascending order
                List<String> partitionValues = tablePartition.getPartitionValues();
                SimpleDateFormat formatter = new SimpleDateFormat(tablePartition.getDatePartitionFormat(), Locale.ROOT);
                long rangeStart = formatter.parse(partitionValues.get(0)).getTime();
                long rangeEnd = formatter.parse(partitionValues.get(partitionValues.size() - 1)).getTime();
                if ((StringUtils.isNotEmpty(startDate)
                        && (Long.parseLong(startDate) < rangeStart || Long.parseLong(startDate) > rangeEnd))
                        || (StringUtils.isNotEmpty(endDate)
                                && (Long.parseLong(endDate) > rangeEnd || Long.parseLong(endDate) < rangeStart))) {
                    String errorMsg = String.format(Locale.ROOT, MsgPicker.getMsg().getTimeExceedPartitionRange(),
                            partitionValues.get(0), partitionValues.get(partitionValues.size() - 1));
                    throw new KylinException(INVALID_INTERNAL_TABLE_PARAMETER, errorMsg);
                }
            }

            logger.info(
                    "create internal table loading job for table: {}, isIncrementBuild: {}, startTime: {}, endTime: {}",
                    internalTable.getIdentity(), isIncremental, startDate, endDate);
            JobParam jobParam = new JobParam().withProject(project).withTable(internalTable.getIdentity())
                    .withYarnQueue(yarnQueue).withJobTypeEnum(jobType).withOwner(BasicService.getUsername())
                    .addExtParams(NBatchConstants.P_INCREMENTAL_BUILD, String.valueOf(isIncremental))
                    .addExtParams(NBatchConstants.P_OUTPUT_MODE, String.valueOf(isRefresh))
                    .addExtParams(NBatchConstants.P_START_DATE, startDate)
                    .addExtParams(NBatchConstants.P_END_DATE, endDate);
            String jobId = getManager(SourceUsageManager.class).licenseCheckWrap(project,
                    () -> getManager(JobManager.class, project).addJob(jobParam));
            jobIds.add(jobId);
            return true;
        }, project);
        String jobName = isRefresh ? INTERNAL_TABLE_REFRESH.toString() : INTERNAL_TABLE_BUILD.toString();
        return InternalTableLoadingJobResponse.of(jobIds, jobName);
    }

    public void dropPartitions(String project, String[] partitionValues, String tableIdentity, String yarnQueue)
            throws IOException {
        // If internal table can not be obtained, will throw a kylin exception
        InternalTableDesc internalTable = checkAndGetInternalTables(project, tableIdentity);
        checkInternalTablePartitions(internalTable, partitionValues);
        InternalTableLoader internalTableLoader = new InternalTableLoader();
        String toBeDelete = String.join(",", partitionValues);
        SparkSession ss = SparderEnv.getSparkSession();
        long start = System.currentTimeMillis();
        logger.info("Start to drop partition for table {}", tableIdentity);
        internalTableLoader.dropPartitions(ss, internalTable, toBeDelete);
        InternalTableLoadJob internalTableLoadJob = new InternalTableLoadJob();
        InternalTableLoadJob.InternalTableMetaUpdateInfo info = internalTableLoadJob.extractUpdateInfo(project,
                internalTable.getIdentity(), getConfig(), ss);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            InternalTableManager internalTableManager = InternalTableManager.getInstance(getConfig(), project);
            InternalTableDesc oldTable = checkAndGetInternalTables(project, tableIdentity);
            InternalTablePartition tablePartition = oldTable.getTablePartition();
            tablePartition.setPartitionValues(info.getPartitionValues());
            tablePartition.setPartitionDetails(info.getPartitionDetails());
            oldTable.setRowCount(info.getFinalCount());
            internalTableManager.saveOrUpdateInternalTable(oldTable);
            return true;
        }, project);

        logger.info("Successfully drop partition[{}] for table {} in {} ms", toBeDelete, tableIdentity,
                System.currentTimeMillis() - start);
    }

    private InternalTableDesc checkAndGetInternalTables(String project, String tableIdentity) {
        InternalTableManager manager = getManager(InternalTableManager.class, project);
        InternalTableDesc internalTable = manager.getInternalTableDesc(tableIdentity);
        if (internalTable == null) {
            throw new KylinException(INTERNAL_TABLE_NOT_EXIST,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getInternalTableNotFound(), tableIdentity));
        }
        return internalTable;
    }

    private InternalTableDesc checkAndGetInternalTables(String project, String table, String database) {
        String dbTableName = database + "." + table;
        return checkAndGetInternalTables(project, dbTableName);
    }

    private void checkInternalTablePartitions(InternalTableDesc internalTable, String[] partitionValues) {
        InternalTablePartition internalTablePartition = internalTable.getTablePartition();
        if (null == internalTablePartition || null == internalTablePartition.getPartitionDetails()) {
            throw new KylinException(INTERNAL_TABLE_PARTITION_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getInternalTablePartitionNotFound(), StringUtils.join(partitionValues, ',')));
        }
        Set<String> partitionValueSet = Sets.newHashSet(partitionValues);
        List<InternalTablePartitionDetail> partitionList = internalTablePartition.getPartitionDetails();
        for (InternalTablePartitionDetail partitionDetail : partitionList) {
            partitionValueSet.remove(partitionDetail.getPartitionValue());
        }
        if (!partitionValueSet.isEmpty()) {
            throw new KylinException(INTERNAL_TABLE_PARTITION_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getInternalTablePartitionNotFound(), StringUtils.join(partitionValueSet, ',')));
        }
    }
}

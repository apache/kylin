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

import static org.apache.kylin.common.exception.ServerErrorCode.COLUMN_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.DATABASE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.ServerErrorCode.SNAPSHOT_MANAGEMENT_NOT_ENABLED;
import static org.apache.kylin.common.exception.ServerErrorCode.SNAPSHOT_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.TABLE_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.JOB_CREATE_CHECK_FAIL;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_BUILD;
import static org.apache.kylin.job.execution.JobTypeEnum.SNAPSHOT_REFRESH;
import static org.apache.kylin.rest.constant.SnapshotStatus.BROKEN;
import static org.apache.kylin.rest.util.TableUtils.calculateTableSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.acl.AclTCRDigest;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.sourceusage.SourceUsageManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.constant.SnapshotStatus;
import org.apache.kylin.rest.request.SnapshotRequest;
import org.apache.kylin.rest.response.JobInfoResponse;
import org.apache.kylin.rest.response.NInitTablesResponse;
import org.apache.kylin.rest.response.SnapshotCheckResponse;
import org.apache.kylin.rest.response.SnapshotColResponse;
import org.apache.kylin.rest.response.SnapshotInfoResponse;
import org.apache.kylin.rest.response.SnapshotPartitionsResponse;
import org.apache.kylin.rest.response.TableNameResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.SneakyThrows;
import lombok.val;

@Component("snapshotService")
public class SnapshotService extends BasicService implements SnapshotSupporter {

    private static final List<String> SNAPSHOT_JOB_TYPES = Lists.newArrayList(SNAPSHOT_BUILD.name(),
            SNAPSHOT_REFRESH.name());
    private static final Logger logger = LoggerFactory.getLogger(SnapshotService.class);
    public static final String IS_REFRESH = "isRefresh";
    public static final String PRIORITY = "priority";
    public static final String YARN_QUEUE = "yarnQueue";
    public static final String TAG = "tag";

    @Autowired
    private AclEvaluate aclEvaluate;
    @Autowired
    private TableService tableService;

    private List<JobInfo> fetchAllRunningSnapshotTasksByTableIds(String project, Set<String> tableIds) {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).fetchNotFinalJobsByTypes(
                project, SNAPSHOT_JOB_TYPES, null == tableIds ? null : Lists.newArrayList(tableIds));
    }

    private List<JobInfo> fetchAllRunningSnapshotTasks(String project, Set<TableDesc> tables) {
        Set<String> tableIds = tables.stream().map(TableDesc::getIdentity).collect(Collectors.toSet());
        return fetchAllRunningSnapshotTasksByTableIds(project, tableIds);
    }

    public JobInfoResponse buildSnapshots(SnapshotRequest snapshotsRequest, boolean isRefresh) {
        if (snapshotsRequest.getDatabases().isEmpty()) {
            return buildSnapshots(snapshotsRequest, isRefresh, snapshotsRequest.getTables());
        }

        Set<String> dbs = snapshotsRequest.getDatabases().stream().map(db -> db.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());
        Map<String, List<TableDesc>> dbToTablesMap = getManager(NTableMetadataManager.class,
                snapshotsRequest.getProject()).dbToTablesMap(getConfig().isStreamingEnabled());

        // check db
        Set<String> nonExisted = dbs.stream().filter(db -> !dbToTablesMap.containsKey(db)).collect(Collectors.toSet());
        if (!nonExisted.isEmpty()) {
            throw new KylinException(DATABASE_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getDatabaseNotExist(), StringUtils.join(nonExisted, ", ")));
        }

        // filter tables need loading
        List<JobInfo> runningSnapshotTasks = fetchAllRunningSnapshotTasksByTableIds(snapshotsRequest.getProject(),
                null);

        Set<String> tables = dbToTablesMap.entrySet().stream() //
                .filter(entry -> dbs.contains(entry.getKey())) //
                .map(Map.Entry::getValue).flatMap(List::stream) //
                .filter(table -> !hasLoadedSnapshot(table, runningSnapshotTasks)) //
                .filter(this::isAuthorizedTableAndColumn) //
                .map(TableDesc::getIdentity).collect(Collectors.toSet());
        snapshotsRequest.getTables().addAll(tables);
        return buildSnapshots(snapshotsRequest, isRefresh, snapshotsRequest.getTables());
    }

    /**
     * Only use to automatic refresh snapshot
     */
    public JobInfoResponse autoRefreshSnapshots(SnapshotRequest snapshotsRequest, boolean isRefresh) {
        val project = snapshotsRequest.getProject();
        val needBuildSnapshotTables = snapshotsRequest.getTables();
        checkSnapshotManualManagement(project);
        Set<TableDesc> tables = checkAndGetTable(project, needBuildSnapshotTables);
        if (isRefresh) {
            checkTableSnapshotExist(project, checkAndGetTable(project, needBuildSnapshotTables));
        }
        checkOptions(tables, snapshotsRequest.getOptions());
        return buildSnapshotsInner(snapshotsRequest, isRefresh, needBuildSnapshotTables, tables);
    }

    @SneakyThrows
    public JobInfoResponse buildSnapshotsInner(SnapshotRequest snapshotsRequest, boolean isRefresh,
            Set<String> needBuildSnapshotTables, Set<TableDesc> tables) {
        val project = snapshotsRequest.getProject();
        val options = snapshotsRequest.getOptions();
        List<String> invalidSnapshotsToBuild = new ArrayList<>();

        invalidSnapshotsToBuild(options, invalidSnapshotsToBuild);

        Map<String, SnapshotRequest.TableOption> finalOptions = Maps.newHashMap();

        //double check for fail fast
        checkRunningSnapshotTask(project, needBuildSnapshotTables);
        JobManager.checkStorageQuota(project);

        List<String> jobIds = new ArrayList<>();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            //double check for fail fast
            checkRunningSnapshotTask(project, needBuildSnapshotTables);
            JobManager.checkStorageQuota(project);
            for (TableDesc tableDesc : tables) {
                JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getConfig(), project);
                jobStatisticsManager.updateStatistics(TimeUtil.getDayStart(System.currentTimeMillis()), 0, 0, 1);
                SnapshotRequest.TableOption option = decideBuildOption(tableDesc, options.get(tableDesc.getIdentity()));
                finalOptions.put(tableDesc.getIdentity(), option);

                logger.info(
                        "create snapshot job with args, table: {}, selectedPartCol: {}, selectedPartition{}, incrementBuild: {},isRefresh: {}",
                        tableDesc.getIdentity(), option.getPartitionCol(), option.getPartitionsToBuild(),
                        option.isIncrementalBuild(), isRefresh);
                JobTypeEnum jobType = isRefresh ? JobTypeEnum.SNAPSHOT_REFRESH : JobTypeEnum.SNAPSHOT_BUILD;
                String value = option.getPartitionsToBuild() == null ? null
                        : JsonUtil.writeValueAsString(option.getPartitionsToBuild());
                JobParam jobParam = new JobParam().withProject(project).withTable(tableDesc.getTableAlias())
                        .withPriority(snapshotsRequest.getPriority()).withYarnQueue(snapshotsRequest.getYarnQueue())
                        .withJobTypeEnum(jobType).withTag(snapshotsRequest.getTag())
                        .withOwner(BasicService.getUsername())
                        .addExtParams(NBatchConstants.P_INCREMENTAL_BUILD, String.valueOf(option.isIncrementalBuild()))
                        .addExtParams(NBatchConstants.P_SELECTED_PARTITION_COL, option.getPartitionCol())
                        .addExtParams(NBatchConstants.P_SELECTED_PARTITION_VALUE, value);
                jobIds.add(getManager(SourceUsageManager.class).licenseCheckWrap(project,
                        () -> getManager(JobManager.class, project).addJob(jobParam)));
            }
            updateTableDesc(project, tables, finalOptions);
            return null;
        }, project);
        String jobName = isRefresh ? SNAPSHOT_REFRESH.toString() : SNAPSHOT_BUILD.toString();
        return JobInfoResponse.of(jobIds, jobName);
    }

    private void updateTableDesc(String project, Set<TableDesc> tables,
            Map<String, SnapshotRequest.TableOption> finalOptions) {
        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        for (TableDesc tableDesc : tables) {
            SnapshotRequest.TableOption option = finalOptions.get(tableDesc.getIdentity());
            if (tableDesc.isSnapshotHasBroken()
                    || !StringUtils.equals(option.getPartitionCol(), tableDesc.getSelectedSnapshotPartitionCol())) {
                TableDesc newTable = tableManager.copyForWrite(tableDesc);
                newTable.setSnapshotHasBroken(false);
                if (!StringUtils.equals(option.getPartitionCol(), tableDesc.getSelectedSnapshotPartitionCol())) {
                    newTable.setSelectedSnapshotPartitionCol(option.getPartitionCol());
                }
                tableManager.updateTableDesc(newTable);
            }
        }
    }

    private static void invalidSnapshotsToBuild(Map<String, SnapshotRequest.TableOption> options,
            List<String> invalidSnapshotsToBuild) {
        for (Map.Entry<String, SnapshotRequest.TableOption> entry : options.entrySet()) {
            Set<String> partitionToBuild = entry.getValue().getPartitionsToBuild();
            if (partitionToBuild != null && partitionToBuild.isEmpty()) {
                invalidSnapshotsToBuild.add(entry.getKey());
            }
        }
        if (!invalidSnapshotsToBuild.isEmpty()) {
            throw new KylinException(INVALID_PARAMETER,
                    MsgPicker.getMsg().getPartitionsToBuildCannotBeEmpty(invalidSnapshotsToBuild));
        }
    }

    public JobInfoResponse buildSnapshots(SnapshotRequest snapshotsRequest, boolean isRefresh,
            Set<String> needBuildSnapshotTables) {
        val project = snapshotsRequest.getProject();
        checkSnapshotManualManagement(project);
        Set<TableDesc> tables = checkAndGetTable(project, needBuildSnapshotTables);
        aclEvaluate.checkProjectOperationPermission(project);
        checkTablePermission(tables);
        if (isRefresh) {
            checkTableSnapshotExist(project, checkAndGetTable(project, needBuildSnapshotTables));
        }
        checkOptions(tables, snapshotsRequest.getOptions());
        return buildSnapshotsInner(snapshotsRequest, isRefresh, needBuildSnapshotTables, tables);
    }

    private void checkOptions(Set<TableDesc> tables, Map<String, SnapshotRequest.TableOption> options) {
        for (TableDesc table : tables) {
            SnapshotRequest.TableOption option = options.get(table.getIdentity());
            if (option != null) {
                String partCol = option.getPartitionCol();
                checkSupportBuildSnapShotByPartition(table);
                if (StringUtils.isNotEmpty(partCol) && table.findColumnByName(partCol) == null) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ROOT, "table %s col %s not exist", table.getIdentity(), partCol));
                }
            }
        }
    }

    private SnapshotRequest.TableOption decideBuildOption(TableDesc tableDesc, SnapshotRequest.TableOption option) {

        boolean incrementalBuild = false;
        String selectedPartCol = null;
        Set<String> partitionsToBuild = null;

        if (option != null) {
            selectedPartCol = StringUtils.isEmpty(option.getPartitionCol()) ? null : option.getPartitionCol();
            incrementalBuild = option.isIncrementalBuild();
            partitionsToBuild = option.getPartitionsToBuild();
        } else {
            if (tableDesc.getLastSnapshotPath() != null) {
                selectedPartCol = tableDesc.getSelectedSnapshotPartitionCol();
                if (tableDesc.getSnapshotPartitionCol() != null) {
                    incrementalBuild = true;
                }
            }
        }
        if (!StringUtils.equals(selectedPartCol, tableDesc.getSnapshotPartitionCol())) {
            incrementalBuild = false;
        }
        return new SnapshotRequest.TableOption(selectedPartCol, incrementalBuild, partitionsToBuild);
    }

    private void checkTablePermission(Set<TableDesc> tables) {
        List<TableDesc> nonPermittedTables = tables.stream().filter(tableDesc -> !isAuthorizedTableAndColumn(tableDesc))
                .collect(Collectors.toList());
        if (!nonPermittedTables.isEmpty()) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getSnapshotOperationPermissionDenied());
        }

    }

    @Transaction(project = 0)
    public SnapshotCheckResponse deleteSnapshots(String project, Set<String> tableNames) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectOperationPermission(project);
        Set<TableDesc> tables = checkAndGetTable(project, tableNames);
        checkTablePermission(tables);
        checkTableSnapshotExist(project, tables);

        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        List<JobInfo> conflictJobs = fetchAllRunningSnapshotTasks(project, tables);
        List<String> conflictJobIds = conflictJobs.stream().map(JobInfo::getJobId).collect(Collectors.toList());
        JobContextUtil.remoteDiscardJob(project, conflictJobIds);
        SnapshotCheckResponse response = new SnapshotCheckResponse();
        conflictJobs.forEach(job -> {
            updateSnapshotCheckResponse(job, response);
        });
        tableNames.forEach(tableName -> {
            TableDesc src = tableManager.getTableDesc(tableName);
            TableDesc copy = tableManager.copyForWrite(src);
            copy.deleteSnapshot(false);

            TableExtDesc ext = tableManager.getOrCreateTableExt(src);
            TableExtDesc extCopy = tableManager.copyForWrite(ext);
            extCopy.setOriginalSize(-1);

            tableManager.mergeAndUpdateTableExt(ext, extCopy);
            tableManager.updateTableDesc(copy);
        });
        return response;
    }

    public SnapshotCheckResponse checkBeforeDeleteSnapshots(String project, Set<String> tableNames) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectOperationPermission(project);
        Set<TableDesc> tables = checkAndGetTable(project, tableNames);
        checkTablePermission(tables);
        checkTableSnapshotExist(project, tables);

        List<JobInfo> conflictJobs = fetchAllRunningSnapshotTasks(project, tables);

        SnapshotCheckResponse response = new SnapshotCheckResponse();
        conflictJobs.forEach(job -> updateSnapshotCheckResponse(job, response));
        return response;
    }

    private void updateSnapshotCheckResponse(JobInfo job, SnapshotCheckResponse response) {
        String tableIdentity = job.getSubject();
        String[] tableSplit = tableIdentity.split("\\.");
        String database = "";
        String table = tableIdentity;
        if (tableSplit.length >= 2) {
            database = tableSplit[0];
            table = tableSplit[1];
        }
        response.addAffectedJobs(job.getJobId(), database, table);
    }

    private void checkTableSnapshotExist(String project, Set<TableDesc> tables) {
        List<JobInfo> executables = fetchAllRunningSnapshotTasks(project, tables);

        List<String> tablesWithEmptySnapshot = tables.stream()
                .filter(tableDesc -> !hasLoadedSnapshot(tableDesc, executables)).map(TableDesc::getIdentity)
                .collect(Collectors.toList());
        if (!tablesWithEmptySnapshot.isEmpty()) {
            throw new KylinException(SNAPSHOT_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getSnapshotNotFound(), StringUtils.join(tablesWithEmptySnapshot, "', '")));
        }
    }

    private void checkSnapshotManualManagement(String project) {
        if (!getManager(NProjectManager.class).getProject(project).getConfig().isSnapshotManualManagementEnabled()) {
            throw new KylinException(SNAPSHOT_MANAGEMENT_NOT_ENABLED,
                    MsgPicker.getMsg().getSnapshotManagementNotEnabled());
        }
    }

    private void checkRunningSnapshotTask(String project, Set<String> needBuildSnapshotTables) {
        //check whether snapshot task is running on current project

        List<JobInfo> executables = fetchAllRunningSnapshotTasksByTableIds(project, needBuildSnapshotTables);

        Set<String> runningTables = new HashSet<>();
        for (JobInfo executable : executables) {
            if (needBuildSnapshotTables.contains(executable.getSubject())) {
                runningTables.add(executable.getSubject());
            }
        }

        if (!runningTables.isEmpty()) {
            JobSubmissionException jobSubmissionException = new JobSubmissionException(JOB_CREATE_CHECK_FAIL);
            runningTables.forEach(tableName -> jobSubmissionException.addJobFailInfo(tableName,
                    new KylinException(JOB_CREATE_CHECK_FAIL)));
            throw jobSubmissionException;
        }

    }

    private Set<TableDesc> checkAndGetTable(String project, Set<String> needBuildSnapshotTables) {
        Preconditions.checkNotNull(needBuildSnapshotTables);
        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        Set<TableDesc> tables = new HashSet<>();
        Set<String> notFoundTables = new HashSet<>();
        for (String tableName : needBuildSnapshotTables) {
            TableDesc tableDesc = tableManager.getTableDesc(tableName);
            if (tableDesc != null) {
                tables.add(tableDesc);
            } else {
                notFoundTables.add(tableName);
            }
        }
        if (!notFoundTables.isEmpty()) {
            throw new KylinException(TABLE_NOT_EXIST, String.format(Locale.ROOT, MsgPicker.getMsg().getTableNotFound(),
                    StringUtils.join(notFoundTables, "', '")));
        }
        return tables;
    }

    @Override
    public Pair<List<SnapshotInfoResponse>, Integer> getProjectSnapshots(String project, String table,
            Set<SnapshotStatus> statusFilter, Set<Boolean> partitionFilter, String sortBy, boolean isReversed,
            Pair<Integer, Integer> offsetAndLimit) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        NTableMetadataManager nTableMetadataManager = getManager(NTableMetadataManager.class, project);

        Pair<String, String> databaseAndTable = checkDatabaseAndTable(table);

        Set<String> groups = getCurrentUserGroups();
        boolean canUseACLGreenChannel = AclPermissionUtil.canUseACLGreenChannel(project, groups);
        Set<String> finalAuthorizedTables = getAclAuthorizedTables(project, canUseACLGreenChannel);

        List<JobInfo> executables = fetchAllRunningSnapshotTasksByTableIds(project, finalAuthorizedTables);

        // Adjust the operation of adding SnapshotInfoResponse and then removing it to
        // first remove the tableDesc that does not meet the conditions, and then add SnapshotInfoResponse
        List<TableDesc> tables = getFilteredTables(nTableMetadataManager, databaseAndTable, canUseACLGreenChannel,
                finalAuthorizedTables, executables, statusFilter, partitionFilter);

        List<SnapshotInfoResponse> response = new ArrayList<>();
        // Here we keep the actual size of tableSnapshots and process only a portion of the data based on paging
        final int returnTableSize = calculateTableSize(offsetAndLimit.getFirst(), offsetAndLimit.getSecond());
        final int actualTableSize = tables.size();
        AtomicInteger satisfiedTableSize = new AtomicInteger();

        tables.forEach(tableDesc -> {
            if (satisfiedTableSize.get() == returnTableSize) {
                return;
            }
            TableExtDesc tableExtDesc = nTableMetadataManager.getOrCreateTableExt(tableDesc);
            Pair<Integer, Integer> countPair = getModelCount(tableDesc);
            response.add(new SnapshotInfoResponse(tableDesc, tableExtDesc, tableDesc.getSnapshotTotalRows(),
                    countPair.getFirst(), countPair.getSecond(), getSnapshotJobStatus(tableDesc, executables),
                    getForbiddenColumns(tableDesc)));
            satisfiedTableSize.getAndIncrement();
        });

        sortBy = StringUtils.isEmpty(sortBy) ? "last_modified_time" : sortBy;
        if ("last_modified_time".equalsIgnoreCase(sortBy) && isReversed) {
            // The reverse order here needs to be cut from the beginning to the end,
            // otherwise the initial data is always returned
            response.sort(SnapshotInfoResponse::compareTo);
            return Pair.newPair(PagingUtil.cutPage(response, 0, offsetAndLimit.getSecond()), actualTableSize);
        } else {
            // Here the positive order needs to be cut from the offset position backwards
            Comparator<SnapshotInfoResponse> comparator = BasicService.propertyComparator(sortBy, !isReversed);
            response.sort(comparator);
            return Pair.newPair(PagingUtil.cutPage(response, offsetAndLimit.getFirst(), offsetAndLimit.getSecond()),
                    actualTableSize);
        }
    }

    public Set<String> getAclAuthorizedTables(String project, boolean canUseACLGreenChannel) {
        Set<String> authorizedTables = new HashSet<>();
        if (!canUseACLGreenChannel) {
            authorizedTables = getAuthorizedTables(project, getManager(AclTCRManager.class, project));
        }
        return authorizedTables;
    }

    public List<TableDesc> getFilteredTables(NTableMetadataManager nTableMetadataManager,
            Pair<String, String> databaseAndTable, boolean canUseACLGreenChannel, Set<String> finalAuthorizedTables,
            List<JobInfo> executables, Set<SnapshotStatus> statusFilter, Set<Boolean> partitionFilter) {
        String finalDatabase = databaseAndTable.getFirst();
        String finalTable = databaseAndTable.getSecond();
        return nTableMetadataManager.listAllTables().stream().filter(tableDesc -> {
            if (StringUtils.isEmpty(finalDatabase)) {
                return true;
            }
            return tableDesc.getDatabase().equalsIgnoreCase(finalDatabase);
        }).filter(tableDesc -> {
            if (StringUtils.isEmpty(finalTable)) {
                return true;
            }
            if (finalDatabase == null
                    && tableDesc.getDatabase().toLowerCase(Locale.ROOT).contains(finalTable.toLowerCase(Locale.ROOT))) {
                return true;
            }
            return tableDesc.getName().toLowerCase(Locale.ROOT).contains(finalTable.toLowerCase(Locale.ROOT));
        }).filter(tableDesc -> {
            if (canUseACLGreenChannel) {
                return true;
            }
            return finalAuthorizedTables.contains(tableDesc.getIdentity());
        }).filter(tableDesc -> hasLoadedSnapshot(tableDesc, executables)).filter(tableDesc -> statusFilter.isEmpty()
                || statusFilter.contains(getSnapshotJobStatus(tableDesc, executables))).filter(tableDesc -> {
                    if (partitionFilter.size() != 1) {
                        return true;
                    }
                    boolean isPartition = partitionFilter.iterator().next();
                    return isPartition != (tableDesc.getSelectedSnapshotPartitionCol() == null);
                }).collect(Collectors.toList());
    }

    private Pair<Integer, Integer> getModelCount(TableDesc tableDesc) {
        int factCount = 0;
        int lookupCount = 0;
        val manager = NDataModelManager.getInstance(getConfig(), tableDesc.getProject());
        for (val model : manager.listAllModels()) {
            if (model.isBroken()) {
                continue;
            }
            if (model.isRootFactTable(tableDesc)) {
                factCount++;
            } else if (model.isLookupTable(tableDesc)) {
                lookupCount++;
            }
        }
        return new Pair<>(factCount, lookupCount);
    }

    private Set<String> getForbiddenColumns(TableDesc tableDesc) {
        String project = tableDesc.getProject();
        Set<String> forbiddenColumns = Sets.newHashSet();
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups)) {
            return forbiddenColumns;
        }

        String username = AclPermissionUtil.getCurrentUsername();
        AclTCRDigest userAuth = getManager(AclTCRManager.class, project).getAuthTablesAndColumns(project, username,
                true);
        Set<String> allColumns = userAuth.getColumns();
        AclTCRDigest groupAuth;
        for (val group : groups) {
            groupAuth = getManager(AclTCRManager.class, project).getAuthTablesAndColumns(project, group, false);
            allColumns.addAll(groupAuth.getColumns());
        }

        forbiddenColumns = Sets.newHashSet(tableDesc.getColumns()).stream()
                .map(columnDesc -> columnDesc.getTable().getIdentity() + "." + columnDesc.getName())
                .collect(Collectors.toSet());

        forbiddenColumns.removeAll(allColumns);
        return forbiddenColumns;
    }

    private SnapshotStatus getSnapshotJobStatus(TableDesc tableDesc, List<JobInfo> executables) {
        if (tableDesc.isSnapshotHasBroken()) {
            return BROKEN;
        }
        boolean hasSnapshot = StringUtils.isNotEmpty(tableDesc.getLastSnapshotPath());
        boolean hasJob = hasRunningJob(tableDesc, executables);
        if (hasSnapshot) {
            if (hasJob) {
                return SnapshotStatus.REFRESHING;
            } else {
                return SnapshotStatus.ONLINE;
            }
        } else {
            if (hasJob) {
                return SnapshotStatus.LOADING;
            } else {
                return SnapshotStatus.OFFLINE;
            }
        }
    }

    private boolean hasRunningJob(TableDesc tableDesc, List<JobInfo> executables) {
        return executables.stream().map(JobInfo::getSubject).collect(Collectors.toList())
                .contains(tableDesc.getIdentity());
    }

    private boolean isAuthorizedTableAndColumn(TableDesc originTable) {
        return isAuthorizedTableAndColumn(originTable, getCurrentUserGroups());
    }

    private boolean isAuthorizedTableAndColumn(TableDesc originTable, Set<String> groups) {
        String project = originTable.getProject();
        if (groups == null) {
            groups = getCurrentUserGroups();
        }
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups)) {
            return true;
        }

        String username = AclPermissionUtil.getCurrentUsername();
        AclTCRDigest userAuth = getManager(AclTCRManager.class, project).getAuthTablesAndColumns(project, username,
                true);
        Set<String> allTables = userAuth.getTables();
        Set<String> allColumns = userAuth.getColumns();
        AclTCRDigest groupAuth;
        for (val group : groups) {
            groupAuth = getManager(AclTCRManager.class, project).getAuthTablesAndColumns(project, group, false);
            allTables.addAll(groupAuth.getTables());
            allColumns.addAll(groupAuth.getColumns());
        }

        if (!allTables.contains(originTable.getIdentity())) {
            return false;
        }

        return allColumns.containsAll(Lists.newArrayList(originTable.getColumns()).stream()
                .map(columnDesc -> columnDesc.getTable().getIdentity() + "." + columnDesc.getName())
                .collect(Collectors.toList()));
    }

    private Set<String> getAuthorizedTables(String project, AclTCRManager aclTCRManager) {
        Set<String> groups = getCurrentUserGroups();

        String username = AclPermissionUtil.getCurrentUsername();
        return Stream
                .concat(Stream.of(Pair.newPair(username, true)),
                        groups.stream().map(group -> Pair.newPair(group, false)))
                .parallel().map(pair -> aclTCRManager
                        .getAuthTablesAndColumns(project, pair.getFirst(), pair.getSecond()).getTables())
                .flatMap(Collection::stream).collect(Collectors.toSet());
    }

    private boolean matchTablePattern(TableDesc tableDesc, String tablePattern, String databasePattern,
            String databaseTarget) {
        if (StringUtils.isEmpty(tablePattern)) {
            return true;
        }

        if (StringUtils.isEmpty(databasePattern)
                && databaseTarget.toLowerCase(Locale.ROOT).contains(tablePattern.toLowerCase(Locale.ROOT))) {
            return true;
        }

        return tableDesc.getName().toLowerCase(Locale.ROOT).contains(tablePattern.toLowerCase(Locale.ROOT));
    }

    public NInitTablesResponse getTables(String project, String tablePattern, int offset, int limit) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);

        String expectedDatabase = null;
        if (tablePattern.contains(".")) {
            expectedDatabase = tablePattern.split("\\.", 2)[0].trim();
            tablePattern = tablePattern.split("\\.", 2)[1].trim();
        }

        // some final variables to filter tables
        String finalTable = tablePattern;
        String finalDatabase = expectedDatabase;
        String finalExpectedDatabase = expectedDatabase;
        boolean streamingEnabled = getConfig().isStreamingEnabled();
        List<JobInfo> jobInfoList = fetchAllRunningSnapshotTasksByTableIds(project, null);
        NInitTablesResponse response = new NInitTablesResponse();
        Set<String> groups = getCurrentUserGroups();
        getManager(NTableMetadataManager.class, project).dbToTablesMap(streamingEnabled).forEach((db, tableList) -> {
            /* If there is no expected database, return a page of table in every database,
             * otherwise, only return a page of table in the specified database.
             */
            if (finalExpectedDatabase != null && !db.equalsIgnoreCase(finalExpectedDatabase))
                return;

            List<TableDesc> tables = tableList.stream()
                    .filter(tableDesc -> matchTablePattern(tableDesc, finalTable, finalDatabase, db))
                    .filter(tableDesc -> isAuthorizedTableAndColumn(tableDesc, groups)) //
                    .filter(table -> table.isAccessible(streamingEnabled)) //
                    .sorted(tableService::compareTableDesc).collect(Collectors.toList());

            int size = tables.size();
            List<TableDesc> pageList = PagingUtil.cutPage(tables, offset, limit);
            if (!pageList.isEmpty()) {
                List<TableNameResponse> tableResponse = pageList.stream()
                        .map(table -> new TableNameResponse(table.getName(), hasLoadedSnapshot(table, jobInfoList)))
                        .collect(Collectors.toList());
                response.putDatabase(db, size, tableResponse);
            }
        });

        return response;
    }

    private boolean hasLoadedSnapshot(TableDesc tableDesc, List<JobInfo> executables) {
        return tableDesc.isSnapshotHasBroken() || StringUtils.isNotEmpty(tableDesc.getLastSnapshotPath())
                || hasRunningJob(tableDesc, executables);
    }

    public List<TableNameResponse> getTableNameResponses(String project, String database, String tablePattern) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        if (tablePattern == null) {
            tablePattern = "";
        }

        List<TableNameResponse> tableNameResponses = new ArrayList<>();
        if (tablePattern.contains(".")) {
            String databasePattern = tablePattern.split("\\.", 2)[0].trim();
            if (!databasePattern.equalsIgnoreCase(database)) {
                return tableNameResponses;
            }
            tablePattern = tablePattern.split("\\.", 2)[1].trim();
        }

        Set<String> groups = getCurrentUserGroups();
        final String finalTable = tablePattern;
        List<TableDesc> tables = tableManager.listAllTables().stream()
                .filter(tableDesc -> tableDesc.getDatabase().equalsIgnoreCase(database)).filter(tableDesc -> {
                    if (StringUtils.isEmpty(finalTable)) {
                        return true;
                    }
                    return tableDesc.getName().toLowerCase(Locale.ROOT).contains(finalTable.toLowerCase(Locale.ROOT));
                }).filter(tableDesc -> isAuthorizedTableAndColumn(tableDesc, groups))
                .sorted(tableService::compareTableDesc).collect(Collectors.toList());
        List<JobInfo> executables = fetchAllRunningSnapshotTasksByTableIds(project,
                tables.stream().map(TableDesc::getIdentity).collect(Collectors.toSet()));
        for (TableDesc tableDesc : tables) {
            TableNameResponse tableNameResponse = new TableNameResponse();
            tableNameResponse.setTableName(tableDesc.getName());
            tableNameResponse.setLoaded(hasLoadedSnapshot(tableDesc, executables));
            tableNameResponses.add(tableNameResponse);
        }
        return tableNameResponses;
    }

    private void checkSupportBuildSnapShotByPartition(ISourceAware sourceAware) {
        ISource source = SourceFactory.getSource(sourceAware);
        if (!source.supportBuildSnapShotByPartition()) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    MsgPicker.getMsg().getJdbcNotSupportPartitionColumnInSnapshot());
        }
    }

    @Transaction(project = 0)
    public void configSnapshotPartitionCol(String project, Map<String, String> table2PartCol) {
        checkSnapshotManualManagement(project);
        checkSupportBuildSnapShotByPartition(getManager(NProjectManager.class).getProject(project));
        aclEvaluate.checkProjectOperationPermission(project);
        checkTableAndCol(project, table2PartCol);

        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        table2PartCol.forEach((tableName, colName) -> {
            TableDesc table = tableManager.copyForWrite(tableManager.getTableDesc(tableName));
            if (StringUtils.isEmpty(colName)) {
                colName = null;
            }
            colName = colName == null ? null : colName.toUpperCase(Locale.ROOT);
            table.setSelectedSnapshotPartitionCol(colName);
            tableManager.updateTableDesc(table);
        });

    }

    private void checkTableAndCol(String project, Map<String, String> table2PartCol) {
        if (table2PartCol.isEmpty()) {
            throw new KylinException(REQUEST_PARAMETER_EMPTY_OR_VALUE_EMPTY, "table_partition_col");
        }
        Set<TableDesc> tables = checkAndGetTable(project, table2PartCol.keySet());
        checkTablePermission(tables);

        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        List<String> notFoundCols = Lists.newArrayList();
        table2PartCol.forEach((tableName, colName) -> {
            TableDesc table = tableManager.getTableDesc(tableName);
            if (StringUtils.isNotEmpty(colName) && table.findColumnByName(colName) == null) {
                notFoundCols.add(tableName + "." + colName);
            }
        });
        if (!notFoundCols.isEmpty()) {
            throw new KylinException(COLUMN_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getColumnNotExist(), StringUtils.join(notFoundCols, "', '")));
        }
    }

    public List<SnapshotColResponse> getSnapshotCol(String project, Set<String> tables, Set<String> databases,
            String tablePattern, boolean includeExistSnapshot) {
        return getSnapshotCol(project, tables, databases, tablePattern, includeExistSnapshot, true);
    }

    public List<SnapshotColResponse> getSnapshotCol(String project, Set<String> tables, Set<String> databases,
            String tablePattern, boolean includeExistSnapshot, boolean excludeBroken) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);

        Set<String> finalTables = Optional.ofNullable(tables).orElse(Sets.newHashSet());
        Set<String> finalDatabase = Optional.ofNullable(databases).orElse(Sets.newHashSet());

        List<TableDesc> allTables = getManager(NTableMetadataManager.class, project).listAllTables().stream()
                .filter(table -> {
                    if (finalDatabase.isEmpty() && finalTables.isEmpty()) {
                        return true;
                    }
                    return finalTables.contains(table.getIdentity()) || finalDatabase.contains(table.getDatabase());
                }).filter(table -> {
                    if (StringUtils.isEmpty(tablePattern)) {
                        return true;
                    }
                    return table.getIdentity().toLowerCase(Locale.ROOT).contains(tablePattern.toLowerCase(Locale.ROOT));
                }).collect(Collectors.toList());

        List<JobInfo> executables = fetchAllRunningSnapshotTasksByTableIds(project,
                allTables.stream().map(TableDesc::getIdentity).collect(Collectors.toSet()));

        return allTables.stream()
                .filter(table -> includeExistSnapshot || !hasLoadedSnapshot(table, executables)
                        || (!excludeBroken && table.isSnapshotHasBroken()))
                .filter(this::isAuthorizedTableAndColumn)
                .map(table -> SnapshotColResponse.from(table, tableSourceTypeTransformer(table)))
                .collect(Collectors.toList());
    }

    public SnapshotColResponse reloadPartitionCol(String project, String table) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        TableDesc newTableDesc = tableService.extractTableMeta(new String[] { table }, project).get(0).getFirst();
        newTableDesc.init(project);
        return SnapshotColResponse.from(newTableDesc, tableSourceTypeTransformer(newTableDesc));
    }

    public Map<String, SnapshotPartitionsResponse> getPartitions(String project, Map<String, String> tablesAndCol) {
        Map<String, SnapshotPartitionsResponse> responses = Maps.newHashMap();
        aclEvaluate.checkProjectReadPermission(project);
        Set<TableDesc> tableDescSet = checkAndGetTable(project, tablesAndCol.keySet());
        checkTablePermission(tableDescSet);
        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        tablesAndCol.forEach((table, v) -> {
            TableDesc tableDesc = tableManager.getTableDesc(table);
            SnapshotPartitionsResponse response = new SnapshotPartitionsResponse();
            List<String> readyPartitions = Lists.newArrayList(tableDesc.getReadyPartitions());
            readyPartitions.sort(String::compareTo);
            response.setReadyPartitions(readyPartitions);
            ISourceMetadataExplorer explr = SourceFactory.getSource(tableDesc).getSourceMetadataExplorer();
            String userSelectPartitionCol = tablesAndCol.get(table);
            if (tableDesc.getPartitionColumn() == null
                    || !tableDesc.getPartitionColumn().equalsIgnoreCase(userSelectPartitionCol)) {
                responses.put(tableDesc.getDatabase() + "." + tableDesc.getName(), null);
                return;
            }
            Set<String> allPartitions = explr.getTablePartitions(tableDesc.getDatabase(), tableDesc.getName(),
                    tableDesc.getProject(), tableDesc.getPartitionColumn());
            allPartitions.removeAll(tableDesc.getReadyPartitions());
            List<String> notReadyPartitions = Lists.newArrayList(allPartitions);
            notReadyPartitions.sort(String::compareTo);
            response.setNotReadyPartitions(notReadyPartitions);
            responses.put(tableDesc.getDatabase() + "." + tableDesc.getName(), response);
        });
        return responses;

    }

    public UnaryOperator<SnapshotColResponse> tableSourceTypeTransformer(TableDesc table) {
        Map<Integer, List<Integer>> sourceProviderFamilyMapping = getConfig().getSourceProviderFamilyMapping();
        return res -> {
            boolean isMainSourceType = sourceProviderFamilyMapping.containsKey(table.getSourceType());
            if (!isMainSourceType) {
                sourceProviderFamilyMapping.entrySet().stream()
                        .filter(entry -> entry.getValue().contains(table.getSourceType())).findFirst()
                        .ifPresent(entry -> res.setSourceType(entry.getKey()));
            }
            return res;
        };
    }
}

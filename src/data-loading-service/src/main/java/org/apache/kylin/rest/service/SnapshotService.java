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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.exception.JobSubmissionException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.manager.JobManager;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.rest.util.PagingUtil;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.engine.spark.job.NSparkSnapshotJob;
import org.apache.kylin.metadata.acl.AclTCRDigest;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;

@Component("snapshotService")
public class SnapshotService extends BasicService implements SnapshotSupporter {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotService.class);

    @Autowired
    private AclEvaluate aclEvaluate;
    @Autowired
    private TableService tableService;

    public JobInfoResponse buildSnapshots(String project, Set<String> buildDatabases,
            Set<String> needBuildSnapshotTables, Map<String, SnapshotRequest.TableOption> options, boolean isRefresh,
            int priority, String yarnQueue, Object tag) {
        if (buildDatabases.isEmpty()) {
            return buildSnapshots(project, needBuildSnapshotTables, options, isRefresh, priority, yarnQueue, tag);
        }

        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        val databases = buildDatabases.stream().map(str -> str.toUpperCase(Locale.ROOT)).collect(Collectors.toSet());
        val databasesNotExist = databases.stream().filter(database -> !tableManager.listDatabases().contains(database))
                .collect(Collectors.toSet());
        if (!databasesNotExist.isEmpty()) {
            throw new KylinException(DATABASE_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getDatabaseNotExist(), StringUtils.join(databasesNotExist, ", ")));
        }
        Set<TableDesc> tablesOfDatabases = tableManager.listAllTables().stream()
                .filter(tableDesc -> databases.contains(tableDesc.getDatabase())).collect(Collectors.toSet());

        tablesOfDatabases = skipLoadedTable(tablesOfDatabases, project);
        tablesOfDatabases = tablesOfDatabases.stream().filter(this::isAuthorizedTableAndColumn)
                .collect(Collectors.toSet());

        needBuildSnapshotTables
                .addAll(tablesOfDatabases.stream().map(TableDesc::getIdentity).collect(Collectors.toSet()));

        return buildSnapshots(project, needBuildSnapshotTables, options, isRefresh, priority, yarnQueue, tag);
    }

    private Set<TableDesc> skipLoadedTable(Set<TableDesc> tablesOfDatabases, String project) {
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);

        return tablesOfDatabases.stream().filter(table -> !hasLoadedSnapshot(table, executables))
                .collect(Collectors.toSet());
    }

    public JobInfoResponse buildSnapshots(String project, Set<String> needBuildSnapshotTables,
            Map<String, SnapshotRequest.TableOption> options, boolean isRefresh, int priority, String yarnQueue,
            Object tag) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectOperationPermission(project);
        Set<TableDesc> tables = checkAndGetTable(project, needBuildSnapshotTables);
        checkTablePermission(tables);
        if (isRefresh) {
            checkTableSnapshotExist(project, checkAndGetTable(project, needBuildSnapshotTables));
        }
        checkOptions(tables, options);

        List<String> invalidSnapshotsToBuild = new ArrayList<>();

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

        Map<String, SnapshotRequest.TableOption> finalOptions = Maps.newHashMap();

        List<String> jobIds = new ArrayList<>();

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            checkRunningSnapshotTask(project, needBuildSnapshotTables);
            JobManager.checkStorageQuota(project);
            getManager(SourceUsageManager.class).licenseCheckWrap(project, () -> {
                for (TableDesc tableDesc : tables) {
                    NExecutableManager execMgr = NExecutableManager.getInstance(getConfig(), project);
                    JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(getConfig(), project);
                    jobStatisticsManager.updateStatistics(TimeUtil.getDayStart(System.currentTimeMillis()), 0, 0, 1);

                    SnapshotRequest.TableOption option = decideBuildOption(tableDesc,
                            options.get(tableDesc.getIdentity()));
                    finalOptions.put(tableDesc.getIdentity(), option);

                    logger.info(
                            "create snapshot job with args, table: {}, selectedPartCol: {}, selectedPartition{}, incrementBuild: {},isRefresh: {}",
                            tableDesc.getIdentity(), option.getPartitionCol(), option.getPartitionsToBuild(),
                            option.isIncrementalBuild(), isRefresh);

                    NSparkSnapshotJob job = NSparkSnapshotJob.create(tableDesc, BasicService.getUsername(),
                            option.getPartitionCol(), option.isIncrementalBuild(), option.getPartitionsToBuild(),
                            isRefresh, yarnQueue, tag);
                    ExecutablePO po = NExecutableManager.toPO(job, project);
                    po.setPriority(priority);
                    execMgr.addJob(po);

                    jobIds.add(job.getId());
                }
                return null;
            });

            NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
            for (TableDesc tableDesc : tables) {
                SnapshotRequest.TableOption option = finalOptions.get(tableDesc.getIdentity());
                if (tableDesc.isSnapshotHasBroken()
                        || !StringUtil.equals(option.getPartitionCol(), tableDesc.getSelectedSnapshotPartitionCol())) {
                    TableDesc newTable = tableManager.copyForWrite(tableDesc);
                    newTable.setSnapshotHasBroken(false);
                    if (!StringUtil.equals(option.getPartitionCol(), tableDesc.getSelectedSnapshotPartitionCol())) {
                        newTable.setSelectedSnapshotPartitionCol(option.getPartitionCol());
                    }
                    tableManager.updateTableDesc(newTable);
                }
            }
            return null;
        }, project);

        String jobName = isRefresh ? SNAPSHOT_REFRESH.toString() : SNAPSHOT_BUILD.toString();
        return JobInfoResponse.of(jobIds, jobName);
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
            List<String> tableIdentities = nonPermittedTables.stream().map(TableDesc::getIdentity)
                    .collect(Collectors.toList());
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

        List<String> needDeleteTables = tables.stream().map(TableDesc::getIdentity).collect(Collectors.toList());

        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        NExecutableManager execManager = getManager(NExecutableManager.class, project);
        val executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, SNAPSHOT_BUILD,
                SNAPSHOT_REFRESH);

        List<AbstractExecutable> conflictJobs = executables.stream()
                .filter(exec -> needDeleteTables.contains(exec.getParam(NBatchConstants.P_TABLE_NAME)))
                .collect(Collectors.toList());

        SnapshotCheckResponse response = new SnapshotCheckResponse();
        conflictJobs.forEach(job -> {
            execManager.discardJob(job.getId());
            updateSnapcheckResponse(job, response);
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

        List<String> needDeleteTables = tables.stream().map(TableDesc::getIdentity).collect(Collectors.toList());

        NExecutableManager execManager = getManager(NExecutableManager.class, project);
        val executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, SNAPSHOT_BUILD,
                SNAPSHOT_REFRESH);

        List<AbstractExecutable> conflictJobs = executables.stream()
                .filter(exec -> needDeleteTables.contains(exec.getParam(NBatchConstants.P_TABLE_NAME)))
                .collect(Collectors.toList());

        SnapshotCheckResponse response = new SnapshotCheckResponse();
        conflictJobs.forEach(job -> updateSnapcheckResponse(job, response));
        return response;
    }

    private void updateSnapcheckResponse(AbstractExecutable job, SnapshotCheckResponse response) {
        String tableIdentity = job.getTargetSubject();
        String[] tableSplit = tableIdentity.split("\\.");
        String database = "";
        String table = tableIdentity;
        if (tableSplit.length >= 2) {
            database = tableSplit[0];
            table = tableSplit[1];
        }
        response.addAffectedJobs(job.getId(), database, table);
    }

    private void checkTableSnapshotExist(String project, Set<TableDesc> tables) {
        NExecutableManager execManager = getManager(NExecutableManager.class, project);
        val executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning, SNAPSHOT_BUILD,
                SNAPSHOT_REFRESH);
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
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);

        Set<String> runningTables = new HashSet<>();
        for (AbstractExecutable executable : executables) {
            if (needBuildSnapshotTables.contains(executable.getParam(NBatchConstants.P_TABLE_NAME))) {
                runningTables.add(executable.getParam(NBatchConstants.P_TABLE_NAME));
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
            throw new KylinException(TABLE_NOT_EXIST, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getTableNotFound(), StringUtils.join(notFoundTables, "', '")));
        }
        return tables;
    }

    @Override
    public List<SnapshotInfoResponse> getProjectSnapshots(String project, String table,
            Set<SnapshotStatus> statusFilter, Set<Boolean> partitionFilter, String sortBy, boolean isReversed) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        NTableMetadataManager nTableMetadataManager = getManager(NTableMetadataManager.class, project);
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);
        if (table == null)
            table = "";

        String database = null;
        if (table.contains(".")) {
            database = table.split("\\.", 2)[0].trim();
            table = table.split("\\.", 2)[1].trim();
        }

        final String finalTable = table;
        final String finalDatabase = database;

        Set<String> groups = getCurrentUserGroups();
        boolean canUseACLGreenChannel = AclPermissionUtil.canUseACLGreenChannel(project, groups, true);
        Set<String> authorizedTables = new HashSet<>();
        if (!canUseACLGreenChannel) {
            authorizedTables = getAuthorizedTables(project, getManager(AclTCRManager.class, project));
        }
        Set<String> finalAuthorizedTables = authorizedTables;
        List<TableDesc> tables = nTableMetadataManager.listAllTables().stream().filter(tableDesc -> {
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
        }).filter(tableDesc -> hasLoadedSnapshot(tableDesc, executables)).collect(Collectors.toList());

        List<SnapshotInfoResponse> response = new ArrayList<>();
        tables.forEach(tableDesc -> {
            Pair<Integer, Integer> countPair = getModelCount(tableDesc);
            response.add(new SnapshotInfoResponse(tableDesc, tableDesc.getSnapshotTotalRows(), countPair.getFirst(),
                    countPair.getSecond(), getSnapshotJobStatus(tableDesc, executables),
                    getForbiddenColumns(tableDesc)));
        });

        if (!statusFilter.isEmpty()) {
            response.removeIf(res -> !statusFilter.contains(res.getStatus()));
        }
        if (partitionFilter.size() == 1) {
            boolean isPartition = partitionFilter.iterator().next();
            response.removeIf(res -> isPartition == (res.getSelectPartitionCol() == null));
        }

        sortBy = StringUtils.isEmpty(sortBy) ? "last_modified_time" : sortBy;
        if ("last_modified_time".equalsIgnoreCase(sortBy) && isReversed) {
            response.sort(SnapshotInfoResponse::compareTo);
        } else {
            Comparator<SnapshotInfoResponse> comparator = BasicService.propertyComparator(sortBy, !isReversed);
            response.sort(comparator);
        }

        return response;
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
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, true)) {
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

    private boolean hasSnapshotOrRunningJob(TableDesc tableDesc, List<AbstractExecutable> executables) {
        return StringUtils.isNotEmpty(tableDesc.getLastSnapshotPath()) || hasRunningJob(tableDesc, executables);
    }

    private SnapshotStatus getSnapshotJobStatus(TableDesc tableDesc, List<AbstractExecutable> executables) {
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

    private boolean hasRunningJob(TableDesc tableDesc, List<AbstractExecutable> executables) {
        return executables.stream().map(executable -> executable.getParam(NBatchConstants.P_TABLE_NAME))
                .collect(Collectors.toList()).contains(tableDesc.getIdentity());
    }

    private boolean isAuthorizedTableAndColumn(TableDesc originTable) {
        String project = originTable.getProject();
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, true)) {
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

    private boolean isAuthorizedTable(TableDesc originTable) {
        String project = originTable.getProject();
        Set<String> groups = getCurrentUserGroups();
        if (AclPermissionUtil.canUseACLGreenChannel(project, groups, true)) {
            return true;
        }

        String username = AclPermissionUtil.getCurrentUsername();
        AclTCRDigest userAuth = getManager(AclTCRManager.class, project).getAuthTablesAndColumns(project, username,
                true);
        Set<String> allTables = userAuth.getTables();
        AclTCRDigest groupAuth;
        for (val group : groups) {
            groupAuth = getManager(AclTCRManager.class, project).getAuthTablesAndColumns(project, group, false);
            allTables.addAll(groupAuth.getTables());
        }

        return allTables.contains(originTable.getIdentity());
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
        NInitTablesResponse response = new NInitTablesResponse();
        NTableMetadataManager nTableMetadataManager = getManager(NTableMetadataManager.class, project);
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);
        Set<String> databases = nTableMetadataManager.listDatabases();

        String expectedDatabase = null;
        if (tablePattern.contains(".")) {
            expectedDatabase = tablePattern.split("\\.", 2)[0].trim();
            tablePattern = tablePattern.split("\\.", 2)[1].trim();
        }

        final String finalTable = tablePattern;
        final String finalDatabase = expectedDatabase;
        for (String database : databases) {
            if (expectedDatabase != null && !expectedDatabase.equalsIgnoreCase(database)) {
                continue;
            }
            List<TableDesc> tables = nTableMetadataManager.listAllTables().stream().filter(tableDesc -> {
                if (StringUtils.isEmpty(database)) {
                    return true;
                }
                return tableDesc.getDatabase().equalsIgnoreCase(database);
            }).filter(tableDesc -> matchTablePattern(tableDesc, finalTable, finalDatabase, database))
                    .filter(this::isAuthorizedTableAndColumn).filter(NTableMetadataManager::isTableAccessible)
                    .sorted(tableService::compareTableDesc).collect(Collectors.toList());

            int tableSize = tables.size();
            List<TableDesc> tablePage = PagingUtil.cutPage(tables, offset, limit);

            if (!tablePage.isEmpty()) {
                List<TableNameResponse> tableResponse = tablePage.stream().map(tableDesc -> {
                    val resp = new TableNameResponse();
                    resp.setTableName(tableDesc.getName());
                    resp.setLoaded(hasLoadedSnapshot(tableDesc, executables));
                    return resp;
                }).collect(Collectors.toList());

                response.putDatabase(database, tableSize, tableResponse);
            }
        }

        return response;
    }

    private boolean hasLoadedSnapshot(TableDesc tableDesc, List<AbstractExecutable> executables) {
        return tableDesc.isSnapshotHasBroken() || hasSnapshotOrRunningJob(tableDesc, executables);
    }

    public List<TableNameResponse> getTableNameResponses(String project, String database, String tablePattern) {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);
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

        final String finalTable = tablePattern;
        List<TableDesc> tables = tableManager.listAllTables().stream()
                .filter(tableDesc -> tableDesc.getDatabase().equalsIgnoreCase(database)).filter(tableDesc -> {
                    if (StringUtils.isEmpty(finalTable)) {
                        return true;
                    }
                    return tableDesc.getName().toLowerCase(Locale.ROOT).contains(finalTable.toLowerCase(Locale.ROOT));
                }).filter(this::isAuthorizedTableAndColumn).sorted(tableService::compareTableDesc)
                .collect(Collectors.toList());
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
        val execManager = NExecutableManager.getInstance(getConfig(), project);
        List<AbstractExecutable> executables = execManager.listExecByJobTypeAndStatus(ExecutableState::isRunning,
                SNAPSHOT_BUILD, SNAPSHOT_REFRESH);

        return getManager(NTableMetadataManager.class, project).listAllTables().stream().filter(table -> {
            if (finalDatabase.isEmpty() && finalTables.isEmpty()) {
                return true;
            }
            return finalTables.contains(table.getIdentity()) || finalDatabase.contains(table.getDatabase());
        }).filter(table -> {
            if (StringUtils.isEmpty(tablePattern)) {
                return true;
            }
            return table.getIdentity().toLowerCase(Locale.ROOT).contains(tablePattern.toLowerCase(Locale.ROOT));
        }).filter(table -> includeExistSnapshot || !hasLoadedSnapshot(table, executables)
                || (!excludeBroken && table.isSnapshotHasBroken())).filter(this::isAuthorizedTableAndColumn)
                .map(SnapshotColResponse::from).collect(Collectors.toList());
    }

    public SnapshotColResponse reloadPartitionCol(String project, String table) throws Exception {
        checkSnapshotManualManagement(project);
        aclEvaluate.checkProjectReadPermission(project);
        TableDesc newTableDesc = tableService.extractTableMeta(Arrays.asList(table).toArray(new String[0]), project)
                .get(0).getFirst();
        newTableDesc.init(project);
        return SnapshotColResponse.from(newTableDesc);
    }

    public Map<String, SnapshotPartitionsResponse> getPartitions(String project, Map<String, String> tablesAndCol) {
        Map<String, SnapshotPartitionsResponse> responses = Maps.newHashMap();
        aclEvaluate.checkProjectReadPermission(project);
        Set<TableDesc> tableDescSet = checkAndGetTable(project, tablesAndCol.keySet());
        checkTablePermission(tableDescSet);
        NTableMetadataManager tableManager = getManager(NTableMetadataManager.class, project);
        for (String table : tablesAndCol.keySet()) {
            TableDesc tableDesc = tableManager.getTableDesc(table);
            SnapshotPartitionsResponse response = new SnapshotPartitionsResponse();
            List<String> readyPartitions = tableDesc.getReadyPartitions().stream().collect(Collectors.toList());
            readyPartitions.sort(String::compareTo);
            response.setReadyPartitions(readyPartitions);
            ISourceMetadataExplorer explr = SourceFactory.getSource(tableDesc).getSourceMetadataExplorer();
            String userSelectPartitionCol = tablesAndCol.get(table);
            if (tableDesc.getPartitionColumn() == null
                    || !tableDesc.getPartitionColumn().equalsIgnoreCase(userSelectPartitionCol)) {
                responses.put(tableDesc.getDatabase() + "." + tableDesc.getName(), null);
                continue;
            }
            Set<String> allPartitions = explr.getTablePartitions(tableDesc.getDatabase(), tableDesc.getName(),
                    tableDesc.getProject(), tableDesc.getPartitionColumn());
            allPartitions.removeAll(tableDesc.getReadyPartitions());
            List<String> notReadyPartitions = allPartitions.stream().collect(Collectors.toList());
            notReadyPartitions.sort(String::compareTo);
            response.setNotReadyPartitions(notReadyPartitions);
            responses.put(tableDesc.getDatabase() + "." + tableDesc.getName(), response);
        }
        return responses;

    }
}

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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_TABLE_NAME;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.EXCLUDED_TABLE_REQUEST_NOT_ALLOWED;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.ONCE_LOAD_TABLE_LIMIT;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.view.LogicalView;
import org.apache.kylin.metadata.view.LogicalViewManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.S3TableExtInfo;
import org.apache.kylin.rest.request.TableExclusionRequest;
import org.apache.kylin.rest.request.TableLoadRequest;
import org.apache.kylin.rest.request.UpdateAWSTableExtDescRequest;
import org.apache.kylin.rest.response.DataResult;
import org.apache.kylin.rest.response.ExcludedColumnResponse;
import org.apache.kylin.rest.response.ExcludedTableDetailResponse;
import org.apache.kylin.rest.response.ExcludedTableResponse;
import org.apache.kylin.rest.response.LoadTableResponse;
import org.apache.kylin.rest.response.TableNameResponse;
import org.apache.kylin.rest.response.UpdateAWSTableExtDescResponse;
import org.apache.kylin.rest.security.KerberosLoginManager;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

@Component("tableExtService")
public class TableExtService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(TableExtService.class);

    public static final int DEFAULT_EXCLUDED_COLUMN_SIZE = 15;

    public static final int ONCE_LOAD_TABLE_LIMIT_COUNT = 1000;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    private AclEvaluate aclEvaluate;

    public LoadTableResponse loadTablesWithShortCircuit(TableLoadRequest request) throws Exception {
        String project = request.getProject();
        aclEvaluate.checkProjectWritePermission(project);
        Set<String> existDbs = Sets.newHashSet(tableService.getSourceDbNames(project));
        int count = 0;
        int dbSize = ArrayUtils.isNotEmpty(request.getDatabases()) ? request.getDatabases().length : 0;
        int tableSize = ArrayUtils.isNotEmpty(request.getTables()) ? request.getTables().length : 0;
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        boolean thresholdEnabled = kylinConfig.isTableLoadThresholdEnabled();
        checkThreshold(thresholdEnabled, tableSize);

        LoadTableResponse tableResponseFromDB = null;
        List<Pair<TableDesc, TableExtDesc>> canLoadTablesFromDB = null;
        Map<String, Set<String>> dbs = new HashMap<>();
        if (dbSize > 0) {
            tableResponseFromDB = new LoadTableResponse();
            StringHelper.toUpperCaseArray(request.getDatabases(), request.getDatabases());
            dbs = classifyDbTables(request.getDatabases(), true);
            Pair<List<Pair<TableDesc, TableExtDesc>>, Integer> pair = findCanLoadTables(dbs, project,
                    true, tableResponseFromDB, existDbs, Maps.newHashMap());
            canLoadTablesFromDB = pair.getFirst();
            count = pair.getSecond();
            checkThreshold(thresholdEnabled, count);
        }

        LoadTableResponse tableResponse = null;
        List<Pair<TableDesc, TableExtDesc>> canLoadTables = null;
        if (tableSize > 0) {
            StringHelper.toUpperCaseArray(request.getTables(), request.getTables());
            Map<String, Set<String>> tables = classifyDbTables(request.getTables(), false);
            tableResponse = new LoadTableResponse();
            Pair<List<Pair<TableDesc, TableExtDesc>>, Integer> pair = findCanLoadTables(tables, project,
                    false, tableResponse, existDbs, dbs);
            canLoadTables = pair.getFirst();
            count = pair.getSecond() + count;
            checkThreshold(thresholdEnabled, count);
        }

        LoadTableResponse loadTableResponse = new LoadTableResponse();
        if (tableResponseFromDB != null) {
            if (!canLoadTablesFromDB.isEmpty()) {
                innerLoadTables(project, tableResponseFromDB, canLoadTablesFromDB);
            }
            loadTableResponse.getFailed().addAll(tableResponseFromDB.getFailed());
            loadTableResponse.getLoaded().addAll(tableResponseFromDB.getLoaded());
            loadTableResponse.getNeedRealSampling().addAll(tableResponseFromDB.getNeedRealSampling());
        }

        if (tableResponse != null) {
            if (!canLoadTables.isEmpty()) {
                innerLoadTables(project, tableResponse, canLoadTables);
            }
            loadTableResponse.getFailed().addAll(tableResponse.getFailed());
            loadTableResponse.getLoaded().addAll(tableResponse.getLoaded());
            loadTableResponse.getNeedRealSampling().addAll(tableResponse.getNeedRealSampling());
        }
        return loadTableResponse;
    }

    private void checkThreshold(boolean thresholdEnabled, int count) {
        if (thresholdEnabled && count > ONCE_LOAD_TABLE_LIMIT_COUNT) {
            throw new KylinException(ONCE_LOAD_TABLE_LIMIT);
        }
    }

    public Pair<List<Pair<TableDesc, TableExtDesc>>, Integer> findCanLoadTables(Map<String, Set<String>> dbTables, String project,
            boolean isDb, LoadTableResponse tableResponse, Set<String> existDbs, Map<String, Set<String>> formalDbs) throws Exception {
        List<Pair<TableDesc, TableExtDesc>> canLoadTables = Lists.newArrayList();
        List<TableNameResponse> responseAll = Lists.newArrayList();
        for (Map.Entry<String, Set<String>> entry : dbTables.entrySet()) {
            String db = entry.getKey();
            Set<String> tableSet = entry.getValue();
            if (!existDbs.contains(db)) {
                if (isDb) {
                    tableResponse.getFailed().add(db);
                } else {
                    List<String> tables = tableSet.stream().map(table -> db + "." + table).collect(Collectors.toList());
                    tableResponse.getFailed().addAll(tables);
                }
                continue;
            }
            Set<String> existTables = Sets.newHashSet(tableService.getSourceTableNames(project, db, ""));
            Set<String> failTables = Sets.newHashSet(tableSet);
            if (!isDb) {
                existTables.retainAll(tableSet);
                failTables.removeAll(existTables);
                List<String> tables = failTables.stream().map(table -> db + "." + table).collect(Collectors.toList());
                tableResponse.getFailed().addAll(tables);
            }

            if (formalDbs.size() == 0 || formalDbs.get(db) == null) {
                String[] tables = existTables.stream().map(table -> db + "." + table).toArray(String[]::new);
                if (tables.length > 0) {
                    filterAccessTables(tables, canLoadTables, tableResponse, project);
                }

                List<TableNameResponse> response = tableService.getHiveTableNameResponses(project, db, "");
                response.forEach(t -> t.setTableName(db + "." + t.getTableName()));
                responseAll.addAll(response);
            }
        }
        return new Pair<>(canLoadTables, getTableCount(responseAll, canLoadTables));
    }

    private int getTableCount(List<TableNameResponse> responseAll,
            List<Pair<TableDesc, TableExtDesc>> canLoadTables) {
        List<String> loaded = responseAll.stream().filter(TableNameResponse::isLoaded)
                .map(TableNameResponse::getTableName).collect(Collectors.toList());
        return (int) canLoadTables.stream().filter(t -> !loaded.contains(t.getFirst().getIdentity())).count();
    }

    public LoadTableResponse loadDbTables(String[] dbTables, String project, boolean isDb) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        Set<String> existDbs = Sets.newHashSet(tableService.getSourceDbNames(project));
        LoadTableResponse tableResponse = new LoadTableResponse();
        Map<String, Set<String>> tables = classifyDbTables(dbTables, isDb);
        List<Pair<TableDesc, TableExtDesc>> canLoadTables = findCanLoadTables(tables, project, isDb, tableResponse,
                existDbs, Maps.newHashMap()).getFirst();
        if (!canLoadTables.isEmpty()) {
            return innerLoadTables(project, tableResponse, canLoadTables);
        }

        return tableResponse;
    }

    @VisibleForTesting
    public void filterAccessTables(
        String[] tables, List<Pair<TableDesc, TableExtDesc>> canLoadTables,
        LoadTableResponse tableResponse, String project) throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        List<Pair<TableDesc, TableExtDesc>> toLoadTables = extractTableMeta(tables, project, tableResponse);
        if (!config.isDDLLogicalViewEnabled()) {
            canLoadTables.addAll(toLoadTables);
            return;
        }
        String viewDB = config.getDDLLogicalViewDB();
        LogicalViewManager viewManager = LogicalViewManager.getInstance(config);
        toLoadTables.stream()
            .filter(table -> !table.getFirst().isLogicalView())
            .forEach(canLoadTables::add);
        toLoadTables.stream()
            .filter(table -> table.getFirst().isLogicalView())
            .forEach(table -> {
                String tableName = table.getFirst().getName();
                LogicalView logicalTable = viewManager.get(tableName);
                String viewProject = logicalTable != null ? logicalTable.getCreatedProject() : "unknown";
                if (logicalTable != null && viewProject.equalsIgnoreCase(project)) {
                    canLoadTables.add(table);
                } else {
                    tableResponse.getFailed().add(viewDB + "." + tableName);
                }
            });
    }

    public LoadTableResponse loadAWSTablesCompatibleCrossAccount(List<S3TableExtInfo> s3TableExtInfoList,
            String project) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        List<String> dbTableList = new ArrayList<>();
        Map<String, S3TableExtInfo> map = new HashMap<>();
        for (S3TableExtInfo s3TableExtInfo : s3TableExtInfoList) {
            dbTableList.add(s3TableExtInfo.getName());
            map.put(s3TableExtInfo.getName(), s3TableExtInfo);
        }
        String[] dbTables = dbTableList.toArray(new String[0]);
        Map<String, Set<String>> dbTableMap = classifyDbTables(dbTables, false);
        Set<String> existDbs = Sets.newHashSet(tableService.getSourceDbNames(project));
        LoadTableResponse tableResponse = new LoadTableResponse();
        List<Pair<TableDesc, TableExtDesc>> loadTables = Lists.newArrayList();
        for (Map.Entry<String, Set<String>> entry : dbTableMap.entrySet()) {
            String db = entry.getKey();
            Set<String> tableSet = entry.getValue();
            if (!existDbs.contains(db)) {
                List<String> tables = tableSet.stream().map(table -> db + "." + table).collect(Collectors.toList());
                tableResponse.getFailed().addAll(tables);
                continue;
            }
            Set<String> existTables = Sets.newHashSet(tableService.getSourceTableNames(project, db, ""));
            Set<String> failTables = Sets.newHashSet(tableSet);
            existTables.retainAll(tableSet);
            failTables.removeAll(existTables);
            List<String> tmpTables = failTables.stream().map(table -> db + "." + table).collect(Collectors.toList());
            tableResponse.getFailed().addAll(tmpTables);

            String[] tables = existTables.stream().map(table -> db + "." + table).toArray(String[]::new);
            if (tables.length > 0) {
                List<Pair<TableDesc, TableExtDesc>> tableDescs = extractTableMeta(tables, project, tableResponse);
                for (Pair<TableDesc, TableExtDesc> tableExtDescPair : tableDescs) {
                    TableDesc tableDesc = tableExtDescPair.getFirst();
                    TableExtDesc tableExtDesc = tableExtDescPair.getSecond();
                    String tableKey = tableDesc.getDatabase() + "." + tableDesc.getName();
                    S3TableExtInfo target = map.get(tableKey);
                    if (null != target) {
                        tableExtDesc.addDataSourceProp(TableExtDesc.S3_ROLE_PROPERTY_KEY, target.getRoleArn());
                        tableExtDesc.addDataSourceProp(TableExtDesc.S3_ENDPOINT_KEY, target.getEndpoint());
                        loadTables.add(tableExtDescPair);
                    } else {
                        tableResponse.getFailed().add(tableKey);
                    }
                }
            }
        }
        if (!loadTables.isEmpty()) {
            return innerLoadTables(project, tableResponse, loadTables);
        }

        return tableResponse;
    }

    public UpdateAWSTableExtDescResponse updateAWSLoadedTableExtProp(UpdateAWSTableExtDescRequest request) {
        aclEvaluate.checkProjectOperationPermission(request.getProject());
        UpdateAWSTableExtDescResponse response = new UpdateAWSTableExtDescResponse();
        Map<String, S3TableExtInfo> map = new HashMap<>();
        for (S3TableExtInfo s3TableExtInfo : request.getTables()) {
            map.put(s3TableExtInfo.getName(), s3TableExtInfo);
        }
        NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, request.getProject());
        List<TableDesc> projectTableDescList = tableMetadataManager.listAllTables();
        List<String> identityList = projectTableDescList.stream().map(TableDesc::getIdentity)
                .filter(identity -> map.get(identity) != null).collect(Collectors.toList());

        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager innerTableMetadataManager = getManager(NTableMetadataManager.class,
                    request.getProject());
            Set<TableExtDesc.S3RoleCredentialInfo> broadcasttedS3Conf = new HashSet<>();
            for (String identity : identityList) {
                TableDesc tableDesc = innerTableMetadataManager.getTableDesc(identity);
                TableExtDesc extDesc = innerTableMetadataManager.getTableExtIfExists(tableDesc);
                if (null == extDesc) {
                    response.getFailed().add(identity);
                } else {
                    TableExtDesc copyExt = innerTableMetadataManager.copyForWrite(extDesc);
                    S3TableExtInfo s3TableExtInfo = map.get(identity);
                    copyExt.addDataSourceProp(TableExtDesc.S3_ROLE_PROPERTY_KEY, s3TableExtInfo.getRoleArn());
                    copyExt.addDataSourceProp(TableExtDesc.S3_ENDPOINT_KEY, s3TableExtInfo.getEndpoint());
                    innerTableMetadataManager.saveTableExt(copyExt);
                    if (!broadcasttedS3Conf.contains(copyExt.getS3RoleCredentialInfo())) {
                        tableService.addAndBroadcastSparkSession(copyExt.getS3RoleCredentialInfo());
                        broadcasttedS3Conf.add(copyExt.getS3RoleCredentialInfo());

                    }
                    response.getSucceed().add(identity);
                }
            }
            return response;

        }, request.getProject());

    }

    private LoadTableResponse innerLoadTables(String project, LoadTableResponse tableResponse,
            List<Pair<TableDesc, TableExtDesc>> loadTables) {
        return EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> { //
            NTableMetadataManager tableManager = NTableMetadataManager.getInstance(KylinConfig.readSystemKylinConfig(),
                    project);
            loadTables.forEach(pair -> {
                String tableName = pair.getFirst().getIdentity();
                boolean success = true;
                boolean realLoaded = false;
                if (tableManager.getTableDesc(tableName) == null) {
                    realLoaded = true;
                    try {
                        loadTable(pair.getFirst(), pair.getSecond(), project);
                    } catch (Exception ex) {
                        logger.error("Failed to load table ({}/{})", project, tableName, ex);
                        success = false;
                    }
                }
                Set<String> targetSet = success ? tableResponse.getLoaded() : tableResponse.getFailed();
                targetSet.add(tableName);
                if (success && realLoaded) {
                    tableResponse.getNeedRealSampling().add(tableName);
                }
            });
            return tableResponse;
        }, project, 1);
    }

    private List<Pair<TableDesc, TableExtDesc>> extractTableMeta(String[] tables, String project,
            LoadTableResponse tableResponse) throws IOException, InterruptedException {
        UserGroupInformation ugi = KerberosLoginManager.getInstance().getProjectUGI(project);
        return ugi.doAs((PrivilegedExceptionAction<List<Pair<TableDesc, TableExtDesc>>>) () -> {
            ProjectInstance projectInstance = getManager(NProjectManager.class).getProject(project);
            List<Pair<TableDesc, TableExtDesc>> extractTableMetas = tableService.extractTableMeta(tables, project);
            if (projectInstance.isProjectKerberosEnabled()) {
                return extractTableMetas.stream().map(pair -> {
                    TableDesc tableDesc = pair.getFirst();
                    String tableName = tableDesc.getIdentity();
                    ISourceMetadataExplorer explr = SourceFactory.getSource(projectInstance)
                            .getSourceMetadataExplorer();
                    if (!explr.checkTablesAccess(Sets.newHashSet(tableName))) {
                        tableResponse.getFailed().add(tableName);
                        return null;
                    }
                    return pair;
                }).filter(Objects::nonNull).collect(Collectors.toList());
            }
            return extractTableMetas;
        });
    }

    private Map<String, Set<String>> classifyDbTables(String[] dbTables, boolean isDb) {
        Map<String, Set<String>> dbTableMap = Maps.newHashMap();
        for (String str : dbTables) {
            String db;
            String table = null;
            if (isDb) {
                db = StringUtils.upperCase(str);
            } else {
                String[] dbTableName = HadoopUtil.parseHiveTableName(str);
                db = StringUtils.upperCase(dbTableName[0]);
                table = StringUtils.upperCase(dbTableName[1]);
            }
            Set<String> tables = dbTableMap.getOrDefault(db, Sets.newHashSet());
            if (table != null) {
                tables.add(table);
            }
            dbTableMap.put(db, tables);
        }
        return dbTableMap;
    }

    /**
     * Load given table to project
     */
    @Transaction(project = 2)
    public void loadTable(TableDesc tableDesc, TableExtDesc extDesc, String project) {
        checkBeforeLoadTable(tableDesc, project);
        String[] loaded = tableService.loadTableToProject(tableDesc, extDesc, project);
        // sanity check when loaded is empty or loaded table is not the table
        String tableName = tableDesc.getIdentity();
        if (loaded.length == 0 || !loaded[0].equals(tableName))
            throw new IllegalStateException();

    }

    @Transaction(project = 1)
    public void removeJobIdFromTableExt(String jobId, String project) {
        aclEvaluate.checkProjectOperationPermission(project);
        NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, project);
        for (TableDesc desc : tableMetadataManager.listAllTables()) {
            TableExtDesc extDesc = tableMetadataManager.getTableExtIfExists(desc);
            if (extDesc == null) {
                continue;
            }
            extDesc = tableMetadataManager.copyForWrite(extDesc);
            if (extDesc.getJodID() != null && jobId.equals(extDesc.getJodID())) {
                extDesc.setJodID(null);
                tableMetadataManager.saveTableExt(extDesc);
            }
        }

    }

    @Transaction(project = 0, retry = 1)
    public void checkAndLoadTable(String project, TableDesc tableDesc, TableExtDesc extDesc) {
        loadTable(tableDesc, extDesc, project);
    }

    private void checkBeforeLoadTable(TableDesc tableDesc, String project) {
        NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, project);
        TableDesc originTableDesc = tableMetadataManager.getTableDesc(tableDesc.getIdentity());
        if (originTableDesc != null && (originTableDesc.getSourceType() == ISourceAware.ID_STREAMING
                || tableDesc.getSourceType() == ISourceAware.ID_STREAMING)) {
            throw new KylinException(INVALID_TABLE_NAME,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getSameTableNameExist(), tableDesc.getIdentity()));
        }
    }

    public List<ExcludedTableResponse> getExcludedTables(String project, boolean viewPartialCols, String tablePattern) {
        aclEvaluate.checkProjectReadPermission(project);
        checkTableExclusionEnabled(project);
        NTableMetadataManager tableMgr = getManager(NTableMetadataManager.class, project);
        return tableMgr.listAllTables().stream().map(tableMgr::getTableExtIfExists) //
                .filter(Objects::nonNull) //
                .filter(tableExt -> tableExt.isExcluded() || !tableExt.getExcludedColumns().isEmpty()) //
                .filter(table -> StringUtils.isBlank(tablePattern)
                        || StringUtils.containsIgnoreCase(table.getIdentity(), tablePattern))
                .map(tableExt -> {
                    TableDesc table = tableMgr.getTableDesc(tableExt.getIdentity());
                    ExcludedTableResponse response = new ExcludedTableResponse();
                    response.setExcluded(tableExt.isExcluded());
                    response.setTable(tableExt.getIdentity());
                    response.setExcludedColSize(tableExt.countExcludedColSize());
                    response.setExcludedColumns(seekExcludedColumns(viewPartialCols, tableExt, table));
                    return response;
                }).collect(Collectors.toList());
    }

    private void checkTableExclusionEnabled(String project) {
        ProjectInstance prjInstance = getManager(NProjectManager.class).getProject(project);
        if (!prjInstance.getConfig().isTableExclusionEnabled()) {
            throw new KylinException(EXCLUDED_TABLE_REQUEST_NOT_ALLOWED, project);
        }
    }

    // excluded columns subject to the order of table columns
    private List<String> seekExcludedColumns(boolean viewPartialCols, TableExtDesc tableExt, TableDesc table) {
        ColumnDesc[] columns = table.getColumns();
        int limit = viewPartialCols ? DEFAULT_EXCLUDED_COLUMN_SIZE : columns.length;
        return Arrays.stream(columns).filter(tableExt::testExcluded) //
                .limit(limit).map(ColumnDesc::getName) //
                .collect(Collectors.toList());
    }

    public ExcludedTableDetailResponse getExcludedTable(String project, String table, int pageOffset, int pageSize,
            String colPattern, boolean matchExcludedCols) {
        aclEvaluate.checkProjectReadPermission(project);
        checkTableExclusionEnabled(project);
        NTableMetadataManager tableMgr = getManager(NTableMetadataManager.class, project);
        TableExtDesc tableExt = tableMgr.getOrCreateTableExt(table);
        List<ColumnDesc> columnList = Arrays.stream(tableMgr.getTableDesc(table).getColumns())
                .filter(column -> StringUtils.isBlank(colPattern)
                        || StringUtils.containsIgnoreCase(column.getName(), colPattern))
                .filter(column -> matchExcludedCols == tableExt.testExcluded(column)) //
                .collect(Collectors.toList());
        DataResult<List<ColumnDesc>> dataResult = DataResult.get(columnList, pageOffset, pageSize);
        List<ExcludedColumnResponse> columns = dataResult.getValue().stream()
                .map(column -> new ExcludedColumnResponse(column, matchExcludedCols)) //
                .collect(Collectors.toList());
        ExcludedTableDetailResponse response = new ExcludedTableDetailResponse();
        response.setTable(tableExt.getIdentity());
        response.setExcluded(tableExt.isExcluded());
        response.setTotalSize(dataResult.getTotalSize());
        response.setOffset(dataResult.getOffset());
        response.setLimit(dataResult.getLimit());
        if (matchExcludedCols) {
            response.setExcludedColumns(columns);
        } else {
            response.setAdmittedColumns(columns);
        }
        return response;
    }

    @Transaction(project = 0)
    public void updateExcludedTables(String project, TableExclusionRequest request) {
        aclEvaluate.checkProjectReadPermission(project);
        checkTableExclusionEnabled(project);

        // update table ext
        NTableMetadataManager tableMgr = getManager(NTableMetadataManager.class, project);
        request.getCanceledTables().stream().map(StringUtils::upperCase).forEach(identity -> {
            boolean tableExtExist = tableMgr.isTableExtExist(identity);
            TableExtDesc tableExt = tableMgr.getOrCreateTableExt(identity);
            tableExt.setIdentity(identity);
            tableExt.setExcluded(false);
            tableExt.getExcludedColumns().clear();
            tableMgr.saveOrUpdateTableExt(tableExtExist, tableExt);
        });
        request.getExcludedTables().forEach(excludedTable -> {
            String excludedTableName = StringUtils.upperCase(excludedTable.getTable());
            boolean tableExtExist = tableMgr.isTableExtExist(excludedTableName);
            TableExtDesc tableExt = tableMgr.getOrCreateTableExt(excludedTableName);
            tableExt.setIdentity(excludedTableName);
            if (excludedTable.isExcluded()) {
                tableExt.getExcludedColumns().clear();
                tableExt.setExcluded(excludedTable.isExcluded());
            } else {
                TableDesc table = tableMgr.getTableDesc(tableExt.getIdentity());
                List<String> toBeRemovedColumns = excludedTable.getRemovedColumns().stream() //
                        .map(StringUtils::upperCase).collect(Collectors.toList());
                List<String> toBeAddedColumns = excludedTable.getAddedColumns().stream() //
                        .map(StringUtils::upperCase).collect(Collectors.toList());
                if (tableExt.isExcluded()) {
                    Set<String> colNameSet = Arrays.stream(table.getColumns()).map(ColumnDesc::getName)
                            .collect(Collectors.toSet());
                    toBeRemovedColumns.forEach(colNameSet::remove);
                    tableExt.getExcludedColumns().addAll(colNameSet);
                    tableExt.setExcluded(excludedTable.isExcluded());
                } else {
                    toBeAddedColumns.stream().map(StringUtils::upperCase).forEach(tableExt.getExcludedColumns()::add);
                    toBeRemovedColumns.forEach(tableExt.getExcludedColumns()::remove);
                    tableExt.setExcluded(excludedTable.isExcluded());
                }
                if (tableExt.getExcludedColumns().size() == table.getColumns().length) {
                    tableExt.getExcludedColumns().clear();
                    tableExt.setExcluded(true);
                }
            }
            tableMgr.saveOrUpdateTableExt(tableExtExist, tableExt);
        });
    }
}

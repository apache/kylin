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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.transaction.AclGrantEventNotifier;
import org.apache.kylin.common.persistence.transaction.AclRevokeEventNotifier;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.acl.DependentColumn;
import org.apache.kylin.metadata.acl.SensitiveDataMask;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import org.apache.kylin.rest.aspect.Transaction;
import org.apache.kylin.rest.request.AccessRequest;
import org.apache.kylin.rest.request.AclTCRRequest;
import org.apache.kylin.rest.response.AclTCRResponse;
import org.apache.kylin.rest.security.MutableAclRecord;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.val;

@Component("aclTCRService")
public class AclTCRService extends BasicService implements AclTCRServiceSupporter {

    private static final Logger logger = LoggerFactory.getLogger(AclTCRService.class);

    private static final String IDENTIFIER_FORMAT = "%s.%s";

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private AccessService accessService;

    @Autowired
    private ProjectService projectService;

    @Autowired
    private UserService userService;

    public void revokeAclTCR(String uuid, String sid, boolean principal) {
        // permission already has been checked in AccessService#revokeAcl
        getManager(NProjectManager.class).listAllProjects().stream().filter(p -> p.getUuid().equals(uuid)).findFirst()
                .ifPresent(prj -> EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    revokePrjAclTCR(prj.getName(), sid, principal);
                    return null;
                }, prj.getName()));
    }

    public void revokeAclTCR(String sid, boolean principal) {
        // only global admin has permission
        // permission already has been checked in UserController, UserGroupController
        projectService.getOwnedProjects().parallelStream()
                .forEach(prj -> EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    revokePrjAclTCR(prj, sid, principal);
                    return null;
                }, prj));
    }

    private void revokePrjAclTCR(String project, String sid, boolean principal) {
        logger.info("revoke project table, column and row acls of project={}, sid={}, principal={}", project, sid,
                principal);
        getManager(AclTCRManager.class, project).revokeAclTCR(sid, principal);
    }

    @Override
    @Transaction(project = 0)
    public void unloadTable(String project, String dbTblName) {
        getManager(AclTCRManager.class, project).unloadTable(dbTblName);
    }

    @Override
    public List<AclTCRResponse> getAclTCRResponse(String project, String sid, boolean principal, boolean authorizedOnly)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        AclTCRManager aclTCRManager = getManager(AclTCRManager.class, project);

        if (hasAdminPermissionInProject(sid, principal, project)) {
            return getAclTCRResponse(project, aclTCRManager.getAllDbAclTable(project));
        }
        AclTCR authorized = aclTCRManager.getAclTCR(sid, principal);
        if (Objects.isNull(authorized)) {
            return Lists.newArrayList();
        }
        if (Objects.isNull(authorized.getTable())) {
            //default all tables were authorized
            return getAclTCRResponse(project, aclTCRManager.getAllDbAclTable(project));
        }
        if (authorizedOnly) {
            return tagTableNum(getAclTCRResponse(project, aclTCRManager.getDbAclTable(project, authorized)),
                    getDbTblColNum(project));
        }
        //all tables with authorized tcr tagged
        return getAllTablesAclTCRResponse(project, aclTCRManager.getDbAclTable(project, authorized));
    }

    @Override
    public boolean hasAdminPermissionInProject(String sid, boolean principal, String project) throws IOException {
        if (principal) {
            if (userService.isGlobalAdmin(sid)) {
                return true;
            }
            val groupsOfUser = accessService.getGroupsOfExecuteUser(sid);
            MutableAclRecord acl = AclPermissionUtil.getProjectAcl(project);
            val groupsInProject = AclPermissionUtil.filterGroupsInProject(groupsOfUser, acl);
            val hasAdminPermission = AclPermissionUtil.isSpecificPermissionInProject(sid, groupsInProject,
                    BasePermission.ADMINISTRATION, acl);
            if (hasAdminPermission) {
                return true;
            }
        } else {
            // role admin group
            if (userGroupService.isAdminGroup(sid)) {
                return true;
            }

            // project admin
            if (AclPermissionUtil.isSpecificPermissionInProject(sid, project, BasePermission.ADMINISTRATION)) {
                return true;
            }
        }
        return false;
    }

    public void updateAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        checkAclTCRRequest(project, requests, sid, principal, true);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            updateAclTCR(project, sid, principal, transformRequests(project, requests));
            return null;
        }, project);
    }

    public void mergeAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests)
            throws IOException {
        aclEvaluate.checkProjectAdminPermission(project);
        checkAclTCRRequest(project, requests, sid, principal, false);
        NTableMetadataManager manager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        AclTCRManager aclTCRManager = getManager(AclTCRManager.class, project);
        AclTCR aclTCR = aclTCRManager.getAclTCR(sid, principal);
        if (aclTCR == null) {
            aclTCR = new AclTCR();
        }
        checkACLTCRRequestRowAuthValid(manager, requests,
                Optional.ofNullable(aclTCR.getTable()).orElse(new AclTCR.Table()));
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            updateAclTCR(project, sid, principal, mergeRequests(project, sid, principal, requests));
            return null;
        }, project);
    }

    private void checkAclTCRRequestDataBaseValid(AclTCRRequest db, Set<String> requestDatabases) {
        Message msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(db.getDatabaseName())) {
            throw new KylinException(ServerErrorCode.EMPTY_PARAMETER, msg.getEmptyDatabaseName());
        }
        db.setDatabaseName(db.getDatabaseName().toUpperCase(Locale.ROOT));
        if (requestDatabases.contains(db.getDatabaseName())) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(Locale.ROOT, msg.getDatabaseParameterDuplicate(), db.getDatabaseName()));
        }
        requestDatabases.add(db.getDatabaseName());
        if (CollectionUtils.isEmpty(db.getTables())) {
            throw new KylinException(ServerErrorCode.EMPTY_PARAMETER, msg.getEmptyTableList());
        }
    }

    private void checkAclTCRRequestTableValid(NTableMetadataManager manager, AclTCRRequest db,
            AclTCRRequest.Table table, Set<String> requestTables, boolean isIncludeAll) {
        Message msg = MsgPicker.getMsg();
        if (StringUtils.isEmpty(table.getTableName())) {
            throw new KylinException(ServerErrorCode.EMPTY_PARAMETER, msg.getEmptyTableName());
        }
        table.setTableName(table.getTableName().toUpperCase(Locale.ROOT));
        String tableName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, db.getDatabaseName(), table.getTableName());
        if (requestTables.contains(tableName)) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(Locale.ROOT, msg.getTableParameterDuplicate(), tableName));
        }
        requestTables.add(tableName);
        if (!isIncludeAll) {
            return;
        }
        if (table.getRows() == null) {
            throw new KylinException(ServerErrorCode.EMPTY_PARAMETER, msg.getEmptyRowList());
        }
        table.getRows().forEach(row -> {
            checkRow(msg, row);
        });
        TableDesc tableDesc = manager.getTableDesc(tableName);
        if (CollectionUtils.isEmpty(table.getColumns()) && tableDesc.getColumns() != null
                && tableDesc.getColumns().length > 0) {
            throw new KylinException(ServerErrorCode.EMPTY_PARAMETER, msg.getEmptyColumnList());
        }
    }

    private static void checkRow(Message msg, AclTCRRequest.Row row) {
        if (StringUtils.isEmpty(row.getColumnName())) {
            throw new KylinException(ServerErrorCode.EMPTY_PARAMETER, msg.getEmptyColumnName());
        }
        if (CollectionUtils.isEmpty(row.getItems())) {
            throw new KylinException(ServerErrorCode.EMPTY_PARAMETER, msg.getEmptyItems());
        }
    }

    private void checkAClTCRRequestParameterValid(NTableMetadataManager manager, Set<String> databases,
            Set<String> tables, Set<String> columns, List<AclTCRRequest> requests, boolean isIncludeAll) {
        Message msg = MsgPicker.getMsg();
        Set<String> requestDatabases = Sets.newHashSet();
        Set<String> requestTables = Sets.newHashSet();
        Set<String> requestColumns = Sets.newHashSet();

        requests.forEach(db -> {
            checkAclTCRRequestDataBaseValid(db, requestDatabases);
            db.getTables().forEach(table -> {
                checkAclTCRRequestTableValid(manager, db, table, requestTables, isIncludeAll);
                String tableName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, db.getDatabaseName(),
                        table.getTableName());
                if (table.getColumns() == null) {
                    return;
                }
                table.getColumns().forEach(column -> {
                    String columnName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, tableName,
                            column.getColumnName());
                    if (StringUtils.isEmpty(column.getColumnName())) {
                        throw new KylinException(ServerErrorCode.EMPTY_PARAMETER, msg.getEmptyColumnName());
                    }
                    column.setColumnName(column.getColumnName().toUpperCase(Locale.ROOT));
                    if (requestColumns.contains(columnName)) {
                        throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                                String.format(Locale.ROOT, msg.getColumnParameterDuplicate(), columnName));
                    }
                    requestColumns.add(columnName);
                });
            });
        });

        if (!isIncludeAll) {
            return;
        }

        val notIncludeDatabase = CollectionUtils.removeAll(databases, requestDatabases);
        if (!notIncludeDatabase.isEmpty()) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, String.format(Locale.ROOT,
                    msg.getDatabaseParameterMissing(), StringUtils.join(notIncludeDatabase, ",")));
        }
        val notIncludeTables = CollectionUtils.removeAll(tables, requestTables);
        if (!notIncludeTables.isEmpty()) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, String.format(Locale.ROOT,
                    msg.getTableParameterMissing(), StringUtils.join(notIncludeTables, ",")));
        }
        val notIncludeColumns = CollectionUtils.removeAll(columns, requestColumns);
        if (!notIncludeColumns.isEmpty()) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER, String.format(Locale.ROOT,
                    msg.getColumnParameterMissing(), StringUtils.join(notIncludeColumns, ",")));
        }
    }

    private void checkAClTCRExist(Set<String> databases, Set<String> tables, Set<String> columns,
            List<AclTCRRequest> requests) {
        Message msg = MsgPicker.getMsg();
        requests.forEach(db -> {
            if (!databases.contains(db.getDatabaseName())) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                        String.format(Locale.ROOT, msg.getDatabaseNotExist(), db.getDatabaseName()));
            }
            db.getTables().forEach(table -> {
                String tableName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, db.getDatabaseName(),
                        table.getTableName());
                if (!tables.contains(tableName)) {
                    throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                            String.format(Locale.ROOT, msg.getTableNotFound(), tableName));
                }
                Optional.ofNullable(table.getRows()).map(List::stream).orElseGet(Stream::empty).forEach(row -> {
                    String columnName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, tableName, row.getColumnName());
                    if (!columns.contains(columnName)) {
                        throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                                String.format(Locale.ROOT, msg.getColumnNotExist(), columnName));
                    }
                });
                Optional.ofNullable(table.getColumns()).map(List::stream).orElseGet(Stream::empty).forEach(column -> {
                    String columnName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, tableName,
                            column.getColumnName());
                    if (!columns.contains(columnName)) {
                        throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                                String.format(Locale.ROOT, msg.getColumnNotExist(), columnName));
                    }
                });
            });
        });

    }

    private void checkAclTCRRequest(String project, List<AclTCRRequest> requests, String sid, boolean principal,
            boolean isIncludeAll) throws IOException {
        Set<String> databases = Sets.newHashSet();
        Set<String> tables = Sets.newHashSet();
        Set<String> columns = Sets.newHashSet();

        NTableMetadataManager manager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val all = manager.listAllTables();
        all.forEach(table -> {
            String dbName = table.getDatabase();
            databases.add(dbName);
            String tbName = table.getIdentity();
            tables.add(tbName);
            Arrays.stream(table.getColumns()).forEach(col -> {
                columns.add(String.format(Locale.ROOT, IDENTIFIER_FORMAT, dbName, col.getIdentity()));
            });
        });
        if (hasAdminPermissionInProject(sid, principal, project)) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    MsgPicker.getMsg().getAdminPermissionUpdateAbandon());
        }
        checkAClTCRRequestParameterValid(manager, databases, tables, columns, requests, isIncludeAll);
        AclTCRManager aclTCRManager = getManager(AclTCRManager.class, project);
        AclTCR aclTCR = aclTCRManager.getAclTCR(sid, principal);
        if (aclTCR == null) {
            aclTCR = new AclTCR();
        }
        checkACLTCRRequestRowAuthValid(manager, requests,
                Optional.ofNullable(aclTCR.getTable()).orElse(new AclTCR.Table()));
        checkAClTCRExist(databases, tables, columns, requests);
    }

    private void checkACLTCRRequestRowAuthValid(NTableMetadataManager manager, List<AclTCRRequest> requests,
            AclTCR.Table aclTables) {
        for (AclTCRRequest request : requests) {
            String database = request.getDatabaseName();
            request.getTables().stream().forEach(table -> checkRowAuthHelper(manager, database, table, aclTables));
        }
    }

    private void checkRowAuthHelper(NTableMetadataManager manager, String database, AclTCRRequest.Table table,
            AclTCR.Table aclTables) {
        boolean requestRlsV1 = table.getRows() != null || table.getLikeRows() != null;
        boolean requestRlsV2 = (table.getRowFilter() != null)
                && CollectionUtils.isNotEmpty(table.getRowFilter().getFilterGroups());
        val columnRow = aclTables.get(database + "." + table.getTableName());
        boolean isV2Used = columnRow != null ? columnRow.getRowFilter() != null : false;

        if (requestRlsV1 && (isV2Used || requestRlsV2)) {
            throw new KylinException(ServerErrorCode.ACL_INVALID_ROW_FIELD,
                    MsgPicker.getMsg().getInvalidRowACLUpdate());
        }

        String tableName = table.getTableName();
        Map<String, String> columnTypes = new HashMap<>();
        TableDesc tableDesc = manager.getTableDesc(database + "." + tableName);
        if (tableDesc == null) {
            return;
        }
        for (ColumnDesc columnDesc : tableDesc.getColumns()) {
            columnTypes.put(columnDesc.getName(), columnDesc.getTypeName());
        }

        Optional.ofNullable(table.getLikeRows()).map(List::stream).orElseGet(Stream::empty)
                .forEach(likeRow -> validateLikeColumnType(likeRow.getColumnName(), columnTypes));

        if (!requestRlsV2) {
            return;
        }

        int filterCount = table.getRowFilter().getFilterGroups().stream().map(AclTCRRequest.FilterGroup::getFilters)
                .map(List::size).reduce(0, Integer::sum);

        final int ROWFILTERTHRESHOLD = KylinConfig.getInstanceFromEnv().getRowFilterLimit();
        if (filterCount > ROWFILTERTHRESHOLD) {
            throw new KylinException(ServerErrorCode.ACL_INVALID_ROW_FIELD, String.format(Locale.ROOT,
                    MsgPicker.getMsg().getRowFilterExceedLimit(), filterCount, ROWFILTERTHRESHOLD));
        }

        table.getRowFilter().getFilterGroups().stream().map(AclTCRRequest.FilterGroup::getFilters).forEach(filters -> {
            for (val filter : filters) {
                int itemCount = Optional.ofNullable(filter.getInItems()).orElse(Lists.newArrayList()).size()
                        + Optional.ofNullable(filter.getLikeItems()).orElse(Lists.newArrayList()).size();
                if (itemCount > ROWFILTERTHRESHOLD) {
                    throw new KylinException(ServerErrorCode.ACL_INVALID_ROW_FIELD,
                            String.format(Locale.ROOT, MsgPicker.getMsg().getRowFilterItemExceedLimit(),
                                    filter.getColumnName(), itemCount, ROWFILTERTHRESHOLD));
                }

                // like clause can only accept type `varchar`, `char` and `string`
                if (CollectionUtils.isEmpty(filter.getLikeItems())) {
                    continue;
                }
                validateLikeColumnType(filter.getColumnName(), columnTypes);
            }
        });
    }

    private void validateLikeColumnType(String columnName, Map<String, String> columnTypes) {
        String type = columnTypes.get(columnName);
        if (type == null) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getColumnNotExist(), columnName));
        }
        if (!type.startsWith("varchar") && !type.equals("string") && !type.startsWith("char")) {
            throw new KylinException(ServerErrorCode.ACL_INVALID_COLUMN_DATA_TYPE,
                    MsgPicker.getMsg().getRowAclNotStringType());
        }
    }

    public void updateAclTCR(String uuid, List<AccessRequest> requests) {
        // permission already has been checked in AccessService#grant, batchGrant
        final boolean defaultAuthorized = KapConfig.getInstanceFromEnv().isProjectInternalDefaultPermissionGranted();
        getManager(NProjectManager.class).listAllProjects().stream().filter(p -> p.getUuid().equals(uuid)).findFirst()
                .ifPresent(prj -> EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                    requests.stream().filter(r -> StringUtils.isNotEmpty(r.getSid())).forEach(r -> {
                        AclTCR aclTCR = new AclTCR();
                        if (!defaultAuthorized && !AclPermissionUtil.isProjectAdminPermission(r.getPermission())) {
                            aclTCR.setTable(new AclTCR.Table());
                        }
                        updateAclTCR(prj.getName(), r.getSid(), r.isPrincipal(), aclTCR);
                    });
                    return null;
                }, prj.getName()));
    }

    private void updateAclTCR(String project, String sid, boolean principal, AclTCR aclTCR) {
        checkDependentColumnUpdate(aclTCR, getManager(AclTCRManager.class, project), sid, principal);
        getManager(AclTCRManager.class, project).updateAclTCR(aclTCR, sid, principal);
    }

    private List<AclTCRResponse.Column> getColumns(String project, String tableIdentity, AclTCR.ColumnRow columnRow,
            boolean isTableAuthorized, AclTCR.ColumnRow authorizedColumnRow) {
        if (Objects.isNull(columnRow) || Objects.isNull(columnRow.getColumn())) {
            return Lists.newArrayList();
        }
        boolean isNull = Objects.isNull(authorizedColumnRow);
        final Map<String, SensitiveDataMask> maskMap = isNull ? new HashMap<>()
                : authorizedColumnRow.getColumnSensitiveDataMaskMap();
        final Map<String, Collection<DependentColumn>> dependentColumnMap = isNull ? new HashMap<>()
                : authorizedColumnRow.getDependentColMap();

        val columnTypeMap = getTableColumnTypeMap(project, tableIdentity);
        return columnRow.getColumn().stream().map(colName -> {
            AclTCRResponse.Column col = new AclTCRResponse.Column();
            col.setColumnName(colName);
            col.setAuthorized(false);
            col.setDatatype(columnTypeMap.get(colName).toString());
            if (isTableAuthorized && (isNull || Objects.isNull(authorizedColumnRow.getColumn()))) {
                col.setAuthorized(true);
            } else if (!isNull && Objects.nonNull(authorizedColumnRow.getColumn())) {
                col.setAuthorized(authorizedColumnRow.getColumn().contains(colName));
            }
            if (maskMap.get(colName) != null) {
                col.setDataMaskType(maskMap.get(colName).getType());
            }
            if (dependentColumnMap.get(col.getColumnName()) != null) {
                col.setDependentColumns(dependentColumnMap.get(col.getColumnName()));
            }
            return col;
        }).collect(Collectors.toList());
    }

    private List<AclTCRResponse.Table> getTables(String project, String databaseName, AclTCR.Table table,
            final AclTCR.Table authorizedTable) {
        if (Objects.isNull(table)) {
            return Lists.newArrayList();
        }
        final boolean nonNull = Objects.nonNull(authorizedTable);
        return table.entrySet().stream().map(te -> {
            AclTCRResponse.Table tbl = new AclTCRResponse.Table();
            tbl.setTableName(te.getKey());
            tbl.setAuthorized(false);
            AclTCR.ColumnRow authorizedColumnRow = null;
            if (nonNull) {
                tbl.setAuthorized(authorizedTable.containsKey(te.getKey()));
                authorizedColumnRow = authorizedTable.get(te.getKey());
            }

            String tableIdentity = String.format(Locale.ROOT, IDENTIFIER_FORMAT, databaseName, te.getKey());

            val columns = getColumns(project, tableIdentity, te.getValue(), tbl.isAuthorized(), authorizedColumnRow);
            tbl.setTotalColumnNum(columns.size());
            tbl.setAuthorizedColumnNum(
                    columns.stream().filter(AclTCRResponse.Column::isAuthorized).mapToInt(i -> 1).sum());
            tbl.setColumns(columns);
            if (Objects.isNull(authorizedColumnRow)) {
                tbl.setRows(Lists.newArrayList());
                tbl.setLikeRows(Lists.newArrayList());
            } else {
                List<AclTCRResponse.Row> notNullRowList = authorizedColumnRow.getRow() == null ? Lists.newArrayList()
                        : transformResponseRow(authorizedColumnRow.getRow());
                tbl.setRows(notNullRowList);
                List<AclTCRResponse.Row> notNullLikeRowList = authorizedColumnRow.getLikeRow() == null
                        ? Lists.newArrayList()
                        : transformResponseRow(authorizedColumnRow.getLikeRow());
                tbl.setLikeRows(notNullLikeRowList);
                if (authorizedColumnRow.getRowFilter() == null) {
                    // AclTCR.rowFilter has not been set.
                    // Convert old `rows` and `like_rows` to `row_filter`.
                    tbl.setRowFilter(
                            transformResponseFromOld(authorizedColumnRow.getRow(), authorizedColumnRow.getLikeRow()));
                } else {
                    tbl.setRowFilter(transformResponseRowFilter(authorizedColumnRow.getRowFilter()));
                }

            }
            return tbl;
        }).collect(Collectors.toList());
    }

    private AclTCRResponse.RowFilter transformResponseFromOld(AclTCR.Row row, AclTCR.Row likeRow) {
        val respRowFilter = new AclTCRResponse.RowFilter();
        row = Optional.ofNullable(row).orElse(new AclTCR.Row());
        likeRow = Optional.ofNullable(likeRow).orElse(new AclTCR.Row());

        if (row.isEmpty() && likeRow.isEmpty()) {
            return respRowFilter;
        }

        Set<String> columns = Sets.newHashSet();
        columns.addAll(row.keySet());
        columns.addAll(likeRow.keySet());

        List<AclTCRResponse.FilterGroup> filterGroups = Lists.newArrayList();
        for (String columnName : columns) {
            val filterGroup = new AclTCRResponse.FilterGroup();
            filterGroup.setGroup(false);
            val filter = new AclTCRResponse.Filter();
            filter.setColumnName(columnName);
            List<String> inList = row.get(columnName) != null ? Lists.newArrayList(row.get(columnName))
                    : Lists.newArrayList();
            filter.setInItems(inList);
            List<String> likeList = likeRow.get(columnName) != null ? Lists.newArrayList(likeRow.get(columnName))
                    : Lists.newArrayList();
            filter.setLikeItems(likeList);
            filterGroup.setFilters(Lists.newArrayList(filter));
            filterGroups.add(filterGroup);
        }

        respRowFilter.setFilterGroups(filterGroups);
        return respRowFilter;
    }

    private List<AclTCRResponse> getAllTablesAclTCRResponse(String project,
            final SortedMap<String, AclTCR.Table> authorized) {
        return getManager(AclTCRManager.class, project).getAllDbAclTable(project).entrySet().stream().map(de -> {
            AclTCRResponse response = new AclTCRResponse();
            response.setDatabaseName(de.getKey());
            response.setAuthorizedTableNum(
                    Objects.isNull(authorized.get(de.getKey())) ? 0 : authorized.get(de.getKey()).size());
            response.setTotalTableNum(de.getValue().size());
            response.setTables(getTables(project, de.getKey(), de.getValue(), authorized.get(de.getKey())));
            return response;
        }).collect(Collectors.toList());
    }

    private List<AclTCRResponse> getAclTCRResponse(String project, SortedMap<String, AclTCR.Table> db2AclTable) {
        return db2AclTable.entrySet().stream().map(de -> {
            AclTCRResponse response = new AclTCRResponse();
            response.setDatabaseName(de.getKey());
            response.setAuthorizedTableNum(de.getValue().size());
            response.setTotalTableNum(de.getValue().size());
            response.setTables(de.getValue().entrySet().stream().map(te -> {
                final Map<String, SensitiveDataMask> maskMap = te.getValue() == null ? new HashMap<>()
                        : te.getValue().getColumnSensitiveDataMaskMap();
                final Map<String, Collection<DependentColumn>> dependentColumnMap = te.getValue() == null
                        ? new HashMap<>()
                        : te.getValue().getDependentColMap();

                String tableIdentity = String.format(Locale.ROOT, IDENTIFIER_FORMAT, de.getKey(), te.getKey());

                val columnTypeMap = getTableColumnTypeMap(project, tableIdentity);
                AclTCRResponse.Table tbl = new AclTCRResponse.Table();
                tbl.setTableName(te.getKey());
                tbl.setAuthorized(true);
                tbl.setTotalColumnNum(te.getValue().getColumn().size());
                tbl.setAuthorizedColumnNum(te.getValue().getColumn().size());
                tbl.setColumns(te.getValue().getColumn().stream().map(colName -> {
                    AclTCRResponse.Column col = new AclTCRResponse.Column();
                    col.setColumnName(colName);
                    col.setDatatype(columnTypeMap.get(colName).toString());
                    col.setAuthorized(true);
                    if (maskMap.get(colName) != null) {
                        col.setDataMaskType(maskMap.get(colName).getType());
                    }
                    if (dependentColumnMap.get(col.getColumnName()) != null) {
                        col.setDependentColumns(dependentColumnMap.get(col.getColumnName()));
                    }
                    return col;
                }).collect(Collectors.toList()));
                tbl.setRows(transformResponseRow(te.getValue().getRow()));
                tbl.setLikeRows(transformResponseRow(te.getValue().getLikeRow()));
                if (te.getValue().getRowFilter() == null) {
                    // AclTCR.rowFilter has not been set.
                    // Convert old `rows` and `like_rows` to `row_filter`.
                    tbl.setRowFilter(transformResponseFromOld(te.getValue().getRow(), te.getValue().getLikeRow()));
                } else {
                    tbl.setRowFilter(transformResponseRowFilter(te.getValue().getRowFilter()));
                }
                return tbl;
            }).collect(Collectors.toList()));
            return response;
        }).collect(Collectors.toList());
    }

    private List<AclTCRResponse.Row> transformResponseRow(AclTCR.Row aclRow) {
        if (MapUtils.isEmpty(aclRow)) {
            return Lists.newArrayList();
        }
        return aclRow.entrySet().stream().filter(e -> Objects.nonNull(e.getValue())).map(entry -> {
            AclTCRResponse.Row row = new AclTCRResponse.Row();
            row.setColumnName(entry.getKey());
            row.setItems(Lists.newArrayList(entry.getValue()));
            return row;
        }).collect(Collectors.toList());
    }

    private AclTCRResponse.RowFilter transformResponseRowFilter(List<AclTCR.FilterGroup> rowFilter) {
        val respRowFilter = new AclTCRResponse.RowFilter();
        if (CollectionUtils.isEmpty(rowFilter)) {
            return respRowFilter;
        }

        respRowFilter.setType(rowFilter.get(0).getType().toString());
        rowFilter.forEach(filterGroup -> {
            val aclFilters = filterGroup.getFilters();
            val respFilterGroups = new AclTCRResponse.FilterGroup();
            respFilterGroups.setGroup(filterGroup.isGroup());
            if (aclFilters != null && aclFilters.size() > 0) {
                val aclFilter = aclFilters.values().iterator().next();
                respFilterGroups.setType(aclFilter.getType().toString());
                List<AclTCRResponse.Filter> respFilters = Lists.newArrayList();
                for (val entry : aclFilters.entrySet()) {
                    val respFilter = new AclTCRResponse.Filter();
                    respFilter.setColumnName(entry.getKey());
                    val aclFilterItem = entry.getValue();
                    respFilter.setInItems(Lists.newArrayList(aclFilterItem.getInItems()));
                    respFilter.setLikeItems(Lists.newArrayList(aclFilterItem.getLikeItems()));
                    respFilters.add(respFilter);
                }
                respFilterGroups.setFilters(respFilters);
            }

            respRowFilter.getFilterGroups().add(respFilterGroups);
        });

        return respRowFilter;
    }

    private List<AclTCRResponse> tagTableNum(List<AclTCRResponse> responses,
            Map<String, Map<String, Integer>> dbTblColNum) {
        responses.forEach(r -> {
            r.setTotalTableNum(dbTblColNum.get(r.getDatabaseName()).size());
            r.getTables().forEach(t -> t.setTotalColumnNum(dbTblColNum.get(r.getDatabaseName()).get(t.getTableName())));
        });
        return responses;
    }

    private void slim(String project, AclTCR aclTCR) {
        if (aclTCR == null || aclTCR.getTable() == null) {
            return;
        }

        aclTCR.getTable().forEach((dbTblName, columnRow) -> {
            if (Objects.isNull(columnRow)) {
                return;
            }

            if (Objects.nonNull(columnRow.getColumn()) && Optional
                    .ofNullable(getManager(NTableMetadataManager.class, project).getTableDesc(dbTblName).getColumns())
                    .map(Arrays::stream).orElseGet(Stream::empty).map(ColumnDesc::getName)
                    .allMatch(colName -> columnRow.getColumn().contains(colName)
                            && columnRow.getColumnSensitiveDataMask() == null
                            && columnRow.getDependentColumns() == null)) {
                columnRow.setColumn(null);
            }

            if (columnRow.isAllRowGranted() && Objects.isNull(columnRow.getColumn())) {
                aclTCR.getTable().put(dbTblName, null);
            }
        });

        if (getManager(NTableMetadataManager.class, project).listAllTables().stream().map(TableDesc::getIdentity)
                .allMatch(dbTblName -> aclTCR.getTable().containsKey(dbTblName))
                && aclTCR.getTable().entrySet().stream().allMatch(e -> Objects.isNull(e.getValue()))) {
            aclTCR.setTable(null);
        }
    }

    private AclTCR transformRequests(String project, List<AclTCRRequest> requests) {
        AclTCR aclTCR = new AclTCR();
        AclTCR.Table aclTable = new AclTCR.Table();
        checkAclRequestParam(project, requests);
        requests.stream().filter(d -> StringUtils.isNotEmpty(d.getDatabaseName())).forEach(d -> d.getTables().stream()
                .filter(t -> t.isAuthorized() && StringUtils.isNotEmpty(t.getTableName())).forEach(t -> {
                    setColumnRow(aclTable, d, t);
                }));

        if (requests.stream().allMatch(d -> Optional.ofNullable(d.getTables()).map(List::stream)
                .orElseGet(Stream::empty).allMatch(AclTCRRequest.Table::isAuthorized))) {
            getManager(NTableMetadataManager.class, project).listAllTables().stream()
                    .filter(t -> !aclTable.containsKey(t.getIdentity())).map(TableDesc::getIdentity)
                    .forEach(dbTblName -> aclTable.put(dbTblName, null));
        }
        aclTCR.setTable(aclTable);
        slim(project, aclTCR);
        checkDependentColumnUpdate(aclTCR);
        return aclTCR;
    }

    private AclTCR mergeRequests(String project, String sid, boolean principal, List<AclTCRRequest> requests) {
        checkAclRequestParam(project, requests);
        AclTCRManager aclTCRManager = getManager(AclTCRManager.class, project);
        AclTCR aclTCR = aclTCRManager.getAclTCR(sid, principal);
        if (aclTCR == null) {
            aclTCR = new AclTCR();
        }

        SortedMap<String, AclTCR.Table> allDbAclTable = aclTCRManager.getAllDbAclTable(project);

        AclTCR.Table aclTable = initTableAcl(aclTCR, allDbAclTable);

        for (AclTCRRequest request : requests) {
            String databaseName = request.getDatabaseName();
            List<AclTCRRequest.Table> tables = request.getTables();
            if (tables == null) {
                continue;
            }
            for (AclTCRRequest.Table table : tables) {
                String tableIdentity = String.format(Locale.ROOT, IDENTIFIER_FORMAT, databaseName,
                        table.getTableName());
                if (!table.isAuthorized()) {
                    // remove table authorized
                    aclTable.remove(String.format(Locale.ROOT, IDENTIFIER_FORMAT, databaseName, table.getTableName()));
                } else {
                    AclTCR.ColumnRow columnRow = aclTable.get(tableIdentity);
                    if (columnRow == null) {
                        columnRow = new AclTCR.ColumnRow();
                        columnRow.setColumn(allDbAclTable.get(databaseName).get(table.getTableName()).getColumn());
                    }

                    updateColumnAcl(columnRow, table.getColumns());
                    if (table.getRowFilter() != null) {
                        table.setRows(Lists.newArrayList());
                        table.setLikeRows(Lists.newArrayList());
                    }

                    updateRowAcl(columnRow, table.getRows(), table.getLikeRows());
                    updateRowFilter(columnRow, table.getRowFilter());

                    aclTable.put(tableIdentity, columnRow);
                }
            }
        }

        aclTCR.setTable(aclTable);
        slim(project, aclTCR);
        checkDependentColumnUpdate(aclTCR);
        checkRowAcl(aclTCR);
        return aclTCR;
    }

    private AclTCR.Table initTableAcl(AclTCR aclTCR, Map<String, AclTCR.Table> allDbAclTable) {
        AclTCR.Table aclTable = aclTCR.getTable();
        if (aclTable == null) {
            aclTable = new AclTCR.Table();
            for (Map.Entry<String, AclTCR.Table> entry : allDbAclTable.entrySet()) {
                for (Map.Entry<String, AclTCR.ColumnRow> tableColumnRowEntry : entry.getValue().entrySet()) {
                    aclTable.put(
                            String.format(Locale.ROOT, IDENTIFIER_FORMAT, entry.getKey(), tableColumnRowEntry.getKey()),
                            tableColumnRowEntry.getValue());
                }
            }

            aclTCR.setTable(aclTable);
        }

        return aclTable;
    }

    private void updateColumnAcl(AclTCR.ColumnRow columnRow, List<AclTCRRequest.Column> columns) {
        if (columns == null) {
            return;
        }

        val masks = new HashSet<>(
                Optional.ofNullable(columnRow.getColumnSensitiveDataMask()).orElse(new ArrayList<>()));

        val dependentColumns = new HashSet<>(
                Optional.ofNullable(columnRow.getDependentColumns()).orElse(new ArrayList<>()));

        for (AclTCRRequest.Column column : columns) {
            masks.removeIf(sensitiveDataMask -> sensitiveDataMask.getColumn().equals(column.getColumnName()));
            dependentColumns.removeIf(dependentColumn -> dependentColumn.getColumn().equals(column.getColumnName()));

            if (column.isAuthorized()) {
                // add to authorized
                if (columnRow.getColumn() != null) {
                    columnRow.getColumn().add(column.getColumnName());
                }

                if (column.getDataMaskType() != null) {
                    masks.add(new SensitiveDataMask(column.getColumnName(), column.getDataMaskType()));
                }

                if (column.getDependentColumns() != null) {
                    for (AclTCRRequest.DependentColumnData dependentColumn : column.getDependentColumns()) {
                        dependentColumns.add(new DependentColumn(column.getColumnName(),
                                dependentColumn.getColumnIdentity(), dependentColumn.getValues()));
                    }
                }
            } else {
                // remove from authorized
                columnRow.getColumn().remove(column.getColumnName());
            }
        }

        columnRow.setColumnSensitiveDataMask(new ArrayList<>(masks));
        columnRow.setDependentColumns(new ArrayList<>(dependentColumns));
    }

    private void updateRowAcl(AclTCR.ColumnRow columnRow, List<AclTCRRequest.Row> rows,
            List<AclTCRRequest.Row> likeRows) {
        if (rows != null) {
            columnRow.setRow(rowConverter(rows));
        }

        if (likeRows != null) {
            columnRow.setLikeRow(rowConverter(likeRows));
        }
    }

    private void updateRowFilter(AclTCR.ColumnRow columnRow, AclTCRRequest.RowFilter rowFilter) {
        if (rowFilter != null) {
            columnRow.setRowFilter(rowFilterConverter(rowFilter));
        }
    }

    private void setColumnRow(AclTCR.Table aclTable, AclTCRRequest req, AclTCRRequest.Table table) {
        String dbTblName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, req.getDatabaseName(), table.getTableName());
        AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
        AclTCR.Column aclColumn;
        if (Optional.ofNullable(table.getColumns()).map(List::stream).orElseGet(Stream::empty).allMatch(
                col -> col.isAuthorized() && col.getDataMaskType() == null && col.getDependentColumns() == null)) {
            aclColumn = null;
        } else {
            aclColumn = new AclTCR.Column();
            aclColumn.addAll(Optional.ofNullable(table.getColumns()).map(List::stream).orElseGet(Stream::empty)
                    .filter(AclTCRRequest.Column::isAuthorized).map(AclTCRRequest.Column::getColumnName)
                    .collect(Collectors.toSet()));
        }
        columnRow.setColumn(aclColumn);

        List<SensitiveDataMask> masks = new LinkedList<>();
        for (AclTCRRequest.Column column : table.getColumns()) {
            if (column.getDataMaskType() != null) {
                masks.add(new SensitiveDataMask(column.getColumnName(), column.getDataMaskType()));
            }
        }
        columnRow.setColumnSensitiveDataMask(masks);

        List<DependentColumn> dependentColumns = new LinkedList<>();
        for (AclTCRRequest.Column column : table.getColumns()) {
            if (column.getDependentColumns() != null) {
                for (AclTCRRequest.DependentColumnData dependentColumn : column.getDependentColumns()) {
                    dependentColumns.add(new DependentColumn(column.getColumnName(),
                            dependentColumn.getColumnIdentity(), dependentColumn.getValues()));
                }
            }
        }
        columnRow.setDependentColumns(dependentColumns);

        AclTCR.Row aclRow = rowConverter(table.getRows());
        columnRow.setRow(aclRow);

        AclTCR.Row aclLikeRow = rowConverter(table.getLikeRows());
        columnRow.setLikeRow(aclLikeRow);

        aclTable.put(dbTblName, columnRow);
    }

    /**
     * Convert request to Acl Filter Group. A FILTER is a FILTER GROUP which only has ONE filter.
     * Filters with same column name within one FILTER GROUP will be merged into one filter.
     * FILTERs with same column name will be merged into one FILTER.
     * FILTERS and filters(in other FILTER GROUPs) will not be merged.
     * @param reqRowFilter
     * @return List<AclTCR.FilterGroup>
     */
    private List<AclTCR.FilterGroup> rowFilterConverter(AclTCRRequest.RowFilter reqRowFilter) {
        List<AclTCR.FilterGroup> aclRowFilter = Lists.newArrayList();
        if (reqRowFilter == null || reqRowFilter.getFilterGroups() == null) {
            return aclRowFilter;
        }
        // Map to remove duplicated FILTERs
        Map<String, AclTCR.FilterItems> uniqueFilter = Maps.newHashMap();
        AclTCR.OperatorType reqGroupType = AclTCR.OperatorType.stringToEnum(reqRowFilter.getType());
        reqRowFilter.getFilterGroups().stream().forEach(reqFilterGroup -> {
            val filterGroup = new AclTCR.FilterGroup();
            filterGroup.setType(reqGroupType);
            filterGroup.setGroup(reqFilterGroup.isGroup());
            val reqFilterType = AclTCR.OperatorType.stringToEnum(reqFilterGroup.getType());
            val filters = new AclTCR.Filters();
            if (reqFilterGroup.isGroup()) {
                // If this is a FILTER GROUP,
                // merge filters with same column name in the same FILTER GROUP
                reqFilterGroup.getFilters().stream()
                        .map(reqFilter -> new AbstractMap.SimpleEntry<>(reqFilter.getColumnName(),
                                new AclTCR.FilterItems(Sets.newTreeSet(reqFilter.getInItems()),
                                        Sets.newTreeSet(reqFilter.getLikeItems()), reqFilterType)))
                        .collect(Collectors.<Map.Entry<String, AclTCR.FilterItems>, String, AclTCR.FilterItems> toMap(
                                Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> AclTCR.FilterItems.merge(v1, v2)))
                        .forEach((columnName, FilterItems) -> filters.put(columnName, FilterItems));
            } else {
                // If this is a FILTER,
                // merge FILTERs with same column name
                val reqFilter = reqFilterGroup.getFilters().get(0);
                val newFilterItem = new AclTCR.FilterItems(Sets.newTreeSet(reqFilter.getInItems()),
                        Sets.newTreeSet(reqFilter.getLikeItems()), reqFilterType);
                if (!uniqueFilter.containsKey(reqFilter.getColumnName())) {
                    // Only put the filter when the column name appears for the first time
                    filters.put(reqFilter.getColumnName(), newFilterItem);
                }
                // Merge filters with duplicated column name
                uniqueFilter.merge(reqFilter.getColumnName(), newFilterItem,
                        (v1, v2) -> AclTCR.FilterItems.merge(v1, v2));
            }

            if (!filters.isEmpty()) {
                filterGroup.setFilters(filters);
                aclRowFilter.add(filterGroup);
            }
        });

        // Update FILTERs
        for (val filterGroup : aclRowFilter) {
            // Skip FILTER GROUPs
            if (filterGroup.isGroup()) {
                continue;
            }
            // Update with values in `uniqueFilter`
            val columnName = filterGroup.getFilters().keySet().iterator().next();
            filterGroup.getFilters().put(columnName, uniqueFilter.get(columnName));
        }

        return aclRowFilter;
    }

    private AclTCR.Row rowConverter(List<AclTCRRequest.Row> requestRows) {
        AclTCR.Row aclRow;
        if (Optional.ofNullable(requestRows).map(List::stream).orElseGet(Stream::empty)
                .allMatch(r -> CollectionUtils.isEmpty(r.getItems()))) {
            aclRow = null;
        } else {
            aclRow = new AclTCR.Row();
            requestRows.stream().filter(r -> CollectionUtils.isNotEmpty(r.getItems()))
                    .map(r -> new AbstractMap.SimpleEntry<>(r.getColumnName(), Sets.newHashSet(r.getItems())))
                    .collect(Collectors.<Map.Entry<String, HashSet<String>>, String, Set<String>> toMap(
                            Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> {
                                v1.addAll(v2);
                                return v1;
                            }))
                    .forEach((colName, rows) -> {
                        AclTCR.RealRow realRow = new AclTCR.RealRow();
                        realRow.addAll(rows);
                        aclRow.put(colName, realRow);
                    });
        }
        return aclRow;
    }

    private void checkDependentColumnUpdate(AclTCR aclTCR, AclTCRManager aclTCRManager, String sid, boolean principal) {
        if (!principal) {
            return;
        }

        Set<String> groups = userGroupService.listUserGroups(sid);
        List<AclTCR> aclTCRs = aclTCRManager.getAclTCRs(sid, groups);
        aclTCRs.add(aclTCR);
        checkDependentColumnUpdate(aclTCRs);
    }

    private void checkDependentColumnUpdate(AclTCR aclTCR) {
        checkDependentColumnUpdate(Lists.newArrayList(aclTCR));
    }

    private void checkRowAcl(AclTCR aclTCR) {
        checkRowAcl(Lists.newArrayList(aclTCR));
    }

    private void checkDependentColumnUpdate(List<AclTCR> aclTCRList) {
        Set<String> dependentColumnIdentities = aclTCRList.stream().filter(Objects::nonNull)
                .filter(acl -> acl.getTable() != null).flatMap(acl -> acl.getTable().values().stream())
                .filter(Objects::nonNull).filter(cr -> cr.getDependentColumns() != null)
                .flatMap(cr -> cr.getDependentColumns().stream()).map(DependentColumn::getDependentColumnIdentity)
                .collect(Collectors.toSet());

        Set<String> dependsOnDependentColumnSet = aclTCRList.stream().filter(Objects::nonNull).map(AclTCR::getTable)
                .filter(Objects::nonNull).map(TreeMap::entrySet).flatMap(Collection::stream)
                .filter(entry -> entry.getValue() != null)
                .filter(entry -> entry.getValue().getDependentColumns() != null)
                .map(entry -> entry.getValue().getDependentColumns().stream()
                        .filter(dependentColumn -> dependentColumnIdentities
                                .contains(entry.getKey() + "." + dependentColumn.getColumn()))
                        .map(DependentColumn::getColumn).collect(Collectors.toSet()))
                .flatMap(Collection::stream).collect(Collectors.toSet());

        if (!dependsOnDependentColumnSet.isEmpty()) {
            throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getNotSupportNestedDependentCol(),
                            String.join(", ", dependsOnDependentColumnSet)));
        }

        for (String dependentColumnIdentity : dependentColumnIdentities) {
            if (aclTCRList.stream().noneMatch(acl -> acl.isColumnAuthorized(dependentColumnIdentity))) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getInvalidColumnAccess(), dependentColumnIdentity));
            }
        }
    }

    private void checkRowAcl(List<AclTCR> aclTCRList) {
        aclTCRList.stream().filter(Objects::nonNull).map(AclTCR::getTable).filter(Objects::nonNull)
                .map(TreeMap::entrySet).flatMap(Collection::stream).filter(entry -> entry.getValue() != null)
                .filter(entry -> entry.getValue().getRow() != null)
                .forEach(entry -> entry.getValue().getRow().keySet().forEach(column -> {
                    if (aclTCRList.stream().noneMatch(acl -> acl.isColumnAuthorized(
                            String.format(Locale.ROOT, IDENTIFIER_FORMAT, entry.getKey(), column)))) {
                        throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                                String.format(Locale.ROOT, MsgPicker.getMsg().getInvalidColumnAccess(), column));
                    }
                }));
    }

    public void checkAclRequestParam(String project, List<AclTCRRequest> requests) {
        NTableMetadataManager tableManager = getTableMetadataManager(project);
        requests.forEach(
                d -> d.getTables().stream().filter(AclTCRRequest.Table::isAuthorized)
                        .filter(table -> !Optional.ofNullable(table.getRows()).map(List::stream)
                                .orElseGet(Stream::empty).allMatch(r -> CollectionUtils.isEmpty(r.getItems())))
                        .forEach(table -> {
                            String tableName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, d.getDatabaseName(),
                                    table.getTableName());
                            TableDesc tableDesc = tableManager.getTableDesc(tableName);
                            table.getRows().stream().filter(r -> CollectionUtils.isNotEmpty(r.getItems()))
                                    .forEach(rows -> {
                                        ColumnDesc columnDesc = tableDesc.findColumnByName(rows.getColumnName());
                                        if (!columnDesc.getType().isNumberFamily()) {
                                            return;
                                        }
                                        String columnName = tableName + "." + rows.getColumnName();
                                        rows.getItems().forEach(item -> {
                                            try {
                                                Double.parseDouble(item);
                                            } catch (Exception e) {
                                                throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                                                        MsgPicker.getMsg().getColumnParameterInvalid(columnName));
                                            }
                                        });

                                    });

                        }));

        requests.forEach(d -> d.getTables().stream().filter(AclTCRRequest.Table::isAuthorized).forEach(table -> {
            String tableName = String.format(Locale.ROOT, IDENTIFIER_FORMAT, d.getDatabaseName(), table.getTableName());
            TableDesc tableDesc = tableManager.getTableDesc(tableName);
            checkSensitiveDataMaskRequest(table, tableDesc);
        }));
    }

    private void checkSensitiveDataMaskRequest(AclTCRRequest.Table table, TableDesc tableDesc) {
        if (table.getColumns() == null) {
            return;
        }
        for (AclTCRRequest.Column column : table.getColumns()) {
            if (column.getDataMaskType() != null && !column.isAuthorized()) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER, String.format(Locale.ROOT,
                        MsgPicker.getMsg().getInvalidColumnAccess(), column.getColumnName()));
            }

            if (column.getDataMaskType() != null && !SensitiveDataMask
                    .isValidDataType(tableDesc.findColumnByName(column.getColumnName()).getDatatype())) {
                throw new KylinException(ServerErrorCode.INVALID_PARAMETER,
                        MsgPicker.getMsg().getInvalidSensitiveDataMaskColumnType());
            }
        }
    }

    private Map<String, Map<String, Integer>> getDbTblColNum(String project) {
        Map<String, Map<String, Integer>> dbTblColNum = Maps.newHashMap();
        getManager(NTableMetadataManager.class, project).listAllTables().forEach(tableDesc -> {
            if (!dbTblColNum.containsKey(tableDesc.getDatabase())) {
                dbTblColNum.put(tableDesc.getDatabase(), Maps.newHashMap());
            }
            dbTblColNum.get(tableDesc.getDatabase()).put(tableDesc.getName(), tableDesc.getColumnCount());
        });
        return dbTblColNum;
    }

    private Map<String, DataType> getTableColumnTypeMap(String project, String tableIdentity) {
        val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        TableDesc tableDesc = tableMetadataManager.getTableDesc(tableIdentity);

        if (tableDesc == null) {
            return Maps.newHashMap();
        }

        return Arrays.stream(tableDesc.getColumns()).collect(Collectors.toMap(ColumnDesc::getName, ColumnDesc::getType,
                (u, v) -> u, () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER)));
    }

    @VisibleForTesting
    public NKylinUserManager getKylinUserManager() {
        return NKylinUserManager.getInstance(getConfig());
    }

    public List<TableDesc> getAuthorizedTables(String project, String user) {
        Set<String> groups = getKylinUserManager().getUserGroups(user);
        return getAuthorizedTables(project, user, groups);
    }

    @VisibleForTesting
    public NTableMetadataManager getTableMetadataManager(String project) {
        Preconditions.checkNotNull(project);
        return NTableMetadataManager.getInstance(getConfig(), project);
    }

    @VisibleForTesting
    boolean canUseACLGreenChannel(String project) {
        return AclPermissionUtil.canUseACLGreenChannel(project, getCurrentUserGroups());
    }

    @VisibleForTesting
    public List<TableDesc> getAuthorizedTables(String project, String user, Set<String> groups) {
        List<AclTCR> aclTCRS = getManager(AclTCRManager.class, project).getAclTCRs(user, groups);
        return getTableMetadataManager(project).listAllTables().stream()
                .filter(tableDesc -> aclTCRS.stream().anyMatch(aclTCR -> aclTCR.isAuthorized(tableDesc.getIdentity())))
                .collect(Collectors.toList());
    }

    public boolean remoteGrantACL(String projectId, List<AccessRequest> accessRequests) throws JsonProcessingException {
        AclGrantEventNotifier notifier = new AclGrantEventNotifier(projectId,
                JsonUtil.writeValueAsString(accessRequests));
        return remoteRequest(notifier, projectId);
    }

    public boolean remoteRevokeACL(String projectId, String sid, boolean principal) {
        AclRevokeEventNotifier notifier = new AclRevokeEventNotifier(projectId, sid, principal);
        return remoteRequest(notifier, projectId);
    }

    public void updateAclFromRemote(AclGrantEventNotifier grantEventNotifier,
            AclRevokeEventNotifier revokeEventNotifier) throws IOException {
        if (grantEventNotifier != null) {
            List<AccessRequest> accessRequest = JsonUtil.readValue(grantEventNotifier.getRawAclTCRRequests(),
                    new TypeReference<List<AccessRequest>>() {
                    });
            updateAclTCR(grantEventNotifier.getProjectId(), accessRequest);
        } else if (revokeEventNotifier != null) {
            revokeAclTCR(revokeEventNotifier.getProjectId(), revokeEventNotifier.getSid(),
                    revokeEventNotifier.isPrincipal());
        }
    }
}

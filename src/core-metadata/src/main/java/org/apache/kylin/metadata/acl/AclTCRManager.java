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

package org.apache.kylin.metadata.acl;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import lombok.val;

public class AclTCRManager {

    private static final Logger logger = LoggerFactory.getLogger(AclTCRManager.class);

    private static final String IDENTIFIER_FORMAT = "%s.%s";

    public static AclTCRManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, AclTCRManager.class);
    }

    static AclTCRManager newInstance(KylinConfig config, String project) {
        return new AclTCRManager(config, project);
    }

    private final KylinConfig config;
    private final String project;

    private CachedCrudAssist<AclTCR> userCrud;

    private CachedCrudAssist<AclTCR> groupCrud;

    public AclTCRManager(KylinConfig config, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing AclGroupManager with KylinConfig Id: {}", System.identityHashCode(config));

        this.config = config;
        this.project = project;

        ResourceStore metaStore = ResourceStore.getKylinMetaStore(this.config);
        userCrud = new CachedCrudAssist<AclTCR>(metaStore, String.format(Locale.ROOT, "/%s/acl/user", project),
                AclTCR.class) {
            @Override
            protected AclTCR initEntityAfterReload(AclTCR acl, String resourceName) {
                acl.init(resourceName);
                return acl;
            }
        };
        userCrud.reloadAll();

        groupCrud = new CachedCrudAssist<AclTCR>(metaStore, String.format(Locale.ROOT, "/%s/acl/group", project),
                AclTCR.class) {
            @Override
            protected AclTCR initEntityAfterReload(AclTCR acl, String resourceName) {
                acl.init(resourceName);
                return acl;
            }
        };
        groupCrud.reloadAll();
    }

    public void unloadTable(String dbTblName) {
        userCrud.listAll().forEach(aclTCR -> {
            if (Objects.isNull(aclTCR.getTable())) {
                return;
            }
            AclTCR copied = userCrud.copyForWrite(aclTCR);
            copied.getTable().remove(dbTblName);
            userCrud.save(copied);
        });

        groupCrud.listAll().forEach(aclTCR -> {
            if (Objects.isNull(aclTCR.getTable())) {
                return;
            }
            AclTCR copied = groupCrud.copyForWrite(aclTCR);
            copied.getTable().remove(dbTblName);
            groupCrud.save(copied);
        });
    }

    public AclTCR getAclTCR(String sid, boolean principal) {
        if (principal) {
            return userCrud.get(sid);
        }
        return groupCrud.get(sid);
    }

    public void updateAclTCR(AclTCR updateTo, String sid, boolean principal) {
        updateTo.init(sid);
        if (principal) {
            doUpdate(updateTo, sid, userCrud);
        } else {
            doUpdate(updateTo, sid, groupCrud);
        }
    }

    private void doUpdate(AclTCR updateTo, String sid, CachedCrudAssist<AclTCR> crud) {
        AclTCR copied;
        AclTCR origin = crud.get(sid);
        if (Objects.isNull(origin)) {
            copied = updateTo;
        } else {
            copied = crud.copyForWrite(origin);
            copied.setTable(updateTo.getTable());
        }
        crud.save(copied);
    }

    public void revokeAclTCR(String sid, boolean principal) {
        if (principal) {
            userCrud.delete(sid);
        } else {
            groupCrud.delete(sid);
        }
    }

    public List<AclTCR> getAclTCRs(String username, Set<String> groups) {
        final List<AclTCR> result = Lists.newArrayList();
        if (StringUtils.isNotEmpty(username)) {
            result.add(userCrud.get(username));
        }
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        boolean batchEnabled = kylinConfig.isBatchGetRowAclEnabled();
        if (CollectionUtils.isNotEmpty(groups)) {
            if (batchEnabled) {
                List<AclTCR> allAclTCR = groupCrud.listAll();
                result.addAll(
                        allAclTCR.stream().filter(t -> groups.contains(t.resourceName())).collect(Collectors.toList()));
            } else {
                groups.forEach(g -> result.add(groupCrud.get(g)));
            }
        }
        return result.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    // [ "DB1.TABLE1" ]
    public Set<String> getAuthorizedTables(String username, Set<String> groups) {
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return getAllTables();
        }
        return all.stream().map(aclTCR -> aclTCR.getTable().keySet()).flatMap(Set::stream).collect(Collectors.toSet());
    }

    // [ DB1.TABLE1.COLUMN1 ]
    public Set<String> getAuthorizedColumns(String username, Set<String> groups) {
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return getAllColumns();
        }
        return all.stream().map(aclTCR -> aclTCR.getTable().entrySet().stream().map(entry -> {
            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(entry.getKey());
            if (Objects.isNull(tableDesc)) {
                return Sets.<String> newHashSet();
            }
            if (Objects.isNull(entry.getValue()) || Objects.isNull(entry.getValue().getColumn())) {
                return Optional.ofNullable(tableDesc.getColumns()).map(Arrays::stream).orElseGet(Stream::empty)
                        .map(columnDesc -> getDbTblCols(tableDesc, columnDesc)).flatMap(Set::stream)
                        .collect(Collectors.toSet());
            }
            return Optional.ofNullable(tableDesc.getColumns()).map(Arrays::stream).orElseGet(Stream::empty)
                    .filter(columnDesc -> entry.getValue().getColumn().contains(columnDesc.getName()))
                    .map(columnDesc -> getDbTblCols(tableDesc, columnDesc)).flatMap(Set::stream)
                    .collect(Collectors.toSet());
        }).flatMap(Set::stream).collect(Collectors.toSet())).flatMap(Set::stream).collect(Collectors.toSet());
    }

    public Multimap<String, String> getAuthorizedColumnsGroupByTable(String userName, Set<String> groups) {
        val allAclTCRs = getAclTCRs(userName, groups);
        if (isTablesAuthorized(allAclTCRs)) {
            return ArrayListMultimap.<String, String> create();
        }

        val authorizedColsMap = HashMultimap.<String, String> create();
        allAclTCRs.stream().forEach(aclTCR -> aclTCR.getTable().entrySet().stream().forEach(entry -> {
            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(entry.getKey());
            if (Objects.isNull(tableDesc) || Objects.isNull(entry.getValue())
                    || Objects.isNull(entry.getValue().getColumn())) {
                return;
            }
            authorizedColsMap.putAll(tableDesc.getIdentity(), entry.getValue().getColumn());
        }));

        return authorizedColsMap;
    }

    public Map<String, List<TableDesc>> getAuthorizedTablesAndColumns(String userName, Set<String> groups,
            boolean fullyAuthorized) {
        if (fullyAuthorized) {
            return NTableMetadataManager.getInstance(config, project).listTablesGroupBySchema();
        }

        val allAclTCRs = getAclTCRs(userName, groups);
        if (isTablesAuthorized(allAclTCRs)) {
            return NTableMetadataManager.getInstance(config, project).listTablesGroupBySchema();
        }

        val schemasMap = Maps.<String, List<TableDesc>> newHashMap();
        allAclTCRs.stream().map(aclTCR -> aclTCR.getTable().keySet()).flatMap(Set::stream).forEach(tableName -> {
            val originTableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
            val authorizedTableDesc = getAuthorizedTableDesc(originTableDesc, allAclTCRs);
            schemasMap.computeIfAbsent(originTableDesc.getDatabase(), value -> Lists.newArrayList());
            schemasMap.get(originTableDesc.getDatabase()).add(authorizedTableDesc);
        });
        return schemasMap;
    }

    public TableDesc getAuthorizedTableDesc(TableDesc originTable, List<AclTCR> allAclTCRs) {
        if (allAclTCRs.stream().noneMatch(aclTCR -> aclTCR.isAuthorized(originTable.getIdentity()))) {
            return null;
        }
        val table = new TableDesc(originTable);
        table.setColumns(Optional.ofNullable(table.getColumns()).map(Arrays::stream).orElseGet(Stream::empty).filter(
                c -> allAclTCRs.stream().anyMatch(aclTCR -> aclTCR.isAuthorized(table.getIdentity(), c.getName())))
                .toArray(ColumnDesc[]::new));
        return table;
    }

    public boolean isColumnsAuthorized(String user, Set<String> groups, Set<String> columns) {
        val allTCRs = getAclTCRs(user, groups);
        for (String column : columns) {
            val columnSplit = column.split("\\.");
            val tableIdentity = columnSplit[0] + "." + columnSplit[1];
            val columnName = columnSplit[2];
            if (allTCRs.stream().noneMatch(aclTCR -> aclTCR.isAuthorized(tableIdentity, columnName)))
                return false;
        }

        return true;
    }

    public Optional<String> failFastUnauthorizedTableColumn(String username, Set<String> groups,
            Map<String, Set<String>> tableColumns) {
        if (MapUtils.isEmpty(tableColumns)) {
            return Optional.empty();
        }
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return Optional.empty();
        }

        final Map<String, Set<String>> authorizedTableColumns = getAuthorizedTableColumns(all);

        //fail-fast
        return tableColumns.entrySet().stream().map(entry -> {
            // fail-fast table
            if (!authorizedTableColumns.containsKey(entry.getKey())) {
                return Optional.of(entry.getKey());
            }
            if (CollectionUtils.isEmpty(entry.getValue())
                    || Objects.isNull(authorizedTableColumns.get(entry.getKey()))) {
                return Optional.<String> empty();
            }
            // fail-fast column
            return entry.getValue().stream()
                    .filter(colName -> !authorizedTableColumns.get(entry.getKey()).contains(colName))
                    .map(colName -> String.format(Locale.ROOT, IDENTIFIER_FORMAT, entry.getKey(), colName)).findAny();
        }).filter(Optional::isPresent).findAny().orElse(Optional.empty());
    }

    public AclTCRDigest getAllUnauthorizedTableColumn(String username, Set<String> groups,
            Map<String, Set<String>> tableColumns) {
        Set<String> unauthTables = Sets.newHashSet();
        Set<String> unauthColumns = Sets.newHashSet();
        if (MapUtils.isEmpty(tableColumns)) {
            return new AclTCRDigest();
        }
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (all == null || all.isEmpty() || isTablesAuthorized(all)) {
            return new AclTCRDigest();
        }

        final Map<String, Set<String>> authorizedTableColumns = getAuthorizedTableColumns(all);

        tableColumns.forEach((table, columns) -> {
            if (!authorizedTableColumns.containsKey(table)) {
                unauthTables.add(table);
                return;
            }
            Set<String> authorizedColumns = authorizedTableColumns.get(table);
            if (CollectionUtils.isEmpty(columns) || Objects.isNull(authorizedColumns)) {
                return;
            }
            for (val column : columns) {
                if (!authorizedColumns.contains(column)) {
                    unauthColumns.add(table + "." + column);
                }
            }
        });
        AclTCRDigest aclTCRDigest = new AclTCRDigest();
        aclTCRDigest.setTables(unauthTables);
        aclTCRDigest.setColumns(unauthColumns);
        return aclTCRDigest;

    }

    public Map<String, String> getTableColumnConcatWhereCondition(String username, Set<String> groups) {
        // <DB1.TABLE1, COLUMN_CONCAT_WHERE_CONDITION>
        Map<String, String> result = Maps.newHashMap();
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return result;
        }

        result = generateCondition(all);
        return result;
    }

    /**
     * Generate table condition string for different tables for different users/groups
     * @param all list of AclTCR
     * @return
     */
    private Map<String, String> generateCondition(List<AclTCR> all) {
        Map<String, String> result = Maps.newHashMap();
        final Map<String, List<PrincipalRowFilter>> dbTblPrincipals = getTblPrincipalSet(all);
        if (MapUtils.isEmpty(dbTblPrincipals)) {
            return result;
        }

        dbTblPrincipals.forEach((dbTblName, principals) -> {
            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(dbTblName);
            if (Objects.isNull(tableDesc)) {
                return;
            }

            final Map<String, String> columnType = Optional.ofNullable(tableDesc.getColumns()).map(Arrays::stream)
                    .orElseGet(Stream::empty)
                    .map(columnDesc -> new AbstractMap.SimpleEntry<>(columnDesc.getName().toUpperCase(),
                            columnDesc.getTypeName()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            List<String> conditions = principals.stream().map(p -> {
                if (CollectionUtils.isEmpty(p.getRowFilter())) {
                    // Fall back to old rls data
                    return generateDbTblCondition(p, columnType);
                } else {
                    // Generate using the row_filter
                    return generateDbTblConditionV2(p, columnType);
                }
            }).collect(Collectors.toList());

            result.put(dbTblName, concatPrincipalConditions(conditions));
        });

        return result;
    }

    /**
     * Generate condition string from old keys 'rows' and 'like_rows' for one table for one user/group
     * @param principalRF
     * @param columnType
     * @return
     */
    private String generateDbTblCondition(PrincipalRowFilter principalRF, Map<String, String> columnType) {
        final ColumnToConds columnConditions = new ColumnToConds();
        principalRF.getRowSets().stream().filter(r -> columnType.containsKey(r.getColumnName()))
                .forEach(r -> columnConditions.put(r.getColumnName(),
                        r.getValues().stream()
                                .map(v -> new ColumnToConds.Cond(v, ColumnToConds.Cond.IntervalType.CLOSED))
                                .collect(Collectors.toList())));
        final ColumnToConds columnLikeConditions = new ColumnToConds();
        principalRF.getLikeRowSets().stream().filter(r -> columnType.containsKey(r.getColumnName()))
                .forEach(r -> columnLikeConditions.put(r.getColumnName(),
                        r.getValues().stream().map(v -> new ColumnToConds.Cond(v, ColumnToConds.Cond.IntervalType.LIKE))
                                .collect(Collectors.toList())));
        return ColumnToConds.concatConds(columnConditions, columnLikeConditions, columnType);
    }

    /**
     * Generate condition string from new key 'row_filter' for one table for one user/group
     * @param principalRF
     * @param columnType
     * @return
     */
    private String generateDbTblConditionV2(PrincipalRowFilter principalRF, Map<String, String> columnType) {
        if (principalRF == null || principalRF.getRowFilter() == null || principalRF.getRowFilter().isEmpty()) {
            return null;
        }

        StrBuilder result = new StrBuilder();
        result.append("(");
        principalRF.getRowFilter().stream().filter(filterGroup -> MapUtils.isNotEmpty(filterGroup.getFilters()))
                .forEach(filterGroup -> {
                    if (result.endsWith(")")) {
                        result.append(" ").append(filterGroup.getType()).append(" ");
                    }

                    result.append("(");
                    filterGroup.getFilters().entrySet().stream()
                            .filter(entry -> CollectionUtils.isNotEmpty(entry.getValue().getInItems())
                                    || CollectionUtils.isNotEmpty(entry.getValue().getLikeItems()))
                            .forEach(entry -> {
                                String columnName = entry.getKey();
                                AclTCR.FilterItems filterItems = entry.getValue();

                                if (result.endsWith(")")) {
                                    result.append(" ").append(filterItems.getType()).append(" ");
                                }

                                result.append("(");
                                String type = Preconditions.checkNotNull(
                                        columnType.get(StringUtils.upperCase(columnName)),
                                        "Column:" + columnName + " type not found");
                                if (CollectionUtils.isNotEmpty(filterItems.getInItems())) {
                                    result.append(columnName).append(" in (").append(Joiner.on(", ")
                                            .join(filterItems.getInItems().stream()
                                                    .map(item -> ColumnToConds.Cond.trimWithoutCheck(item, type))
                                                    .collect(Collectors.toList())))
                                            .append(")");
                                }

                                if (CollectionUtils.isNotEmpty(filterItems.getLikeItems())) {
                                    if (result.endsWith(")")) {
                                        result.append(" OR ");
                                    }

                                    result.append(Joiner.on(" OR ")
                                            .join(filterItems.getLikeItems().stream()
                                                    .map(item -> columnName + " like "
                                                            + ColumnToConds.Cond.trimWithoutCheck(item, type))
                                                    .collect(Collectors.toList())));
                                }

                                result.append(")");
                            });

                    result.append(")");
                });

        result.append(")");
        return result.toString();
    }

    private String concatPrincipalConditions(List<String> conditions) {
        final String joint = String.join(" OR ", conditions);
        if (conditions.size() > 1) {
            StringBuilder sb = new StringBuilder("(");
            sb.append(joint);
            sb.append(")");
            return sb.toString();
        }
        return joint;
    }

    public AclTCR.ColumnRealRows getAuthorizedRows(String dbTblName, String colName, List<AclTCR> aclTCRS) {
        AclTCR.RealRow authEqualRows = new AclTCR.RealRow();
        AclTCR.RealRow authLikeRows = new AclTCR.RealRow();
        for (AclTCR aclTCR : aclTCRS) {
            if (Objects.isNull(aclTCR.getTable())) {
                return new AclTCR.ColumnRealRows();
            }

            if (!aclTCR.getTable().containsKey(dbTblName)) {
                continue;
            }

            AclTCR.ColumnRow columnRow = aclTCR.getTable().get(dbTblName);

            if (Objects.isNull(columnRow)) {
                return new AclTCR.ColumnRealRows();
            }

            AclTCR.Row equalRow = columnRow.getRow();
            AclTCR.Row likeRow = columnRow.getLikeRow();

            if ((equalRow == null || equalRow.get(colName) == null)
                    && (likeRow == null || likeRow.get(colName) == null)) {
                return new AclTCR.ColumnRealRows();
            }

            if (equalRow != null && equalRow.get(colName) != null) {
                authEqualRows.addAll(equalRow.get(colName));
            }

            if (likeRow != null && likeRow.get(colName) != null) {
                authLikeRows.addAll(likeRow.get(colName));
            }
        }
        String dbTblColName = dbTblName + "." + colName;
        return new AclTCR.ColumnRealRows(dbTblColName, authEqualRows, authLikeRows);
    }

    /**
     * Check whether this AclTCR user/group has all row access of table
     * @param dbTblName
     * @param aclTcr
     * @return
     */
    private static boolean isColumnWithoutRowLimit(String dbTblName, final AclTCR aclTcr) {
        if (!aclTcr.getTable().containsKey(dbTblName)) {
            return false;
        }
        AclTCR.ColumnRow columnRow = aclTcr.getTable().get(dbTblName);
        return Objects.isNull(columnRow) || columnRow.isAllRowGranted();
    }

    private Map<String, List<PrincipalRowFilter>> getTblPrincipalSet(final List<AclTCR> acls) {
        final Map<String, List<PrincipalRowFilter>> dbTblPrincipals = Maps.newHashMap();
        acls.forEach(tcr -> tcr.getTable().forEach((dbTblName, columnRow) -> {
            if (Objects.isNull(columnRow) || (Objects.isNull(columnRow.getRow())
                    && Objects.isNull(columnRow.getLikeRow()) && Objects.isNull(columnRow.getRowFilter()))) {
                return;
            }
            final PrincipalRowFilter dbTblPrincipalRF = new PrincipalRowFilter();
            if (columnRow.getRowFilter() != null) {
                updatePrincipalRowFilter(columnRow.getRowFilter(), dbTblPrincipalRF, acls, tcr, dbTblName);
            } else {
                updatePrincipalRowSet(columnRow.getRow(), dbTblPrincipalRF.getRowSets(), acls, tcr, dbTblName);
                updatePrincipalRowSet(columnRow.getLikeRow(), dbTblPrincipalRF.getLikeRowSets(), acls, tcr, dbTblName);
            }
            if (CollectionUtils.isEmpty(dbTblPrincipalRF.getRowSets())
                    && CollectionUtils.isEmpty(dbTblPrincipalRF.getLikeRowSets())
                    && CollectionUtils.isEmpty(dbTblPrincipalRF.getRowFilter())) {
                return;
            }
            if (!dbTblPrincipals.containsKey(dbTblName)) {
                dbTblPrincipals.put(dbTblName, Lists.newArrayList());
            }
            dbTblPrincipals.get(dbTblName).add(dbTblPrincipalRF);
        }));
        return dbTblPrincipals;
    }

    /**
     * Update AclTCR.Row when no other acl users/groups can access all rows upon table
     * @param sourceRow
     * @param destRowSet
     * @param acls
     * @param currentAcl
     * @param dbTblName
     */
    private static void updatePrincipalRowSet(AclTCR.Row sourceRow, List<RowSet> destRowSet, final List<AclTCR> acls,
            final AclTCR currentAcl, final String dbTblName) {
        if (Objects.isNull(sourceRow)) {
            return;
        }
        sourceRow.forEach((colName, realRow) -> {
            if (Objects.isNull(realRow) || acls.stream().filter(e -> !e.equals(currentAcl))
                    .anyMatch(e -> isColumnWithoutRowLimit(dbTblName, e))) {
                // If another acl user/group has all row access upon this table,
                // then do not add row filter
                return;
            }
            destRowSet.add(new RowSet(colName, realRow));
        });
    }

    /**
     * Update AclTCR.RowFilter when no other acl users/groups can access all rows upon table
     * @param rowFilters
     * @param principalRF
     * @param acls
     * @param currentAcl
     * @param dbTblName
     */
    private void updatePrincipalRowFilter(List<AclTCR.FilterGroup> rowFilters, PrincipalRowFilter principalRF,
            final List<AclTCR> acls, final AclTCR currentAcl, final String dbTblName) {
        if (Objects.isNull(rowFilters) || acls.stream().filter(e -> !e.equals(currentAcl))
                .anyMatch(e -> isColumnWithoutRowLimit(dbTblName, e))) {
            // If another acl user/group has all row access upon this table,
            // then do not add row filter
            return;
        }

        principalRF.getRowFilter().addAll(rowFilters);
    }

    private boolean isTablesAuthorized(List<AclTCR> all) {
        //default all tables were authorized
        return all.stream().anyMatch(aclTCR -> Objects.isNull(aclTCR.getTable()));
    }

    private Set<String> getAllTables() {
        return NTableMetadataManager.getInstance(config, project).listAllTables().stream().map(TableDesc::getIdentity)
                .collect(Collectors.toSet());
    }

    private Set<String> getAllColumns() {
        return NTableMetadataManager.getInstance(config, project).listAllTables().stream()
                .map(tableDesc -> Optional.ofNullable(tableDesc.getColumns()).map(Arrays::stream)
                        .orElseGet(Stream::empty).map(columnDesc -> getDbTblCols(tableDesc, columnDesc))
                        .flatMap(Set::stream).collect(Collectors.toSet()))
                .flatMap(Set::stream).collect(Collectors.toSet());
    }

    private Set<String> getDbTblCols(TableDesc tableDesc, ColumnDesc columnDesc) {
        Set<String> result = Sets.newHashSet();
        if (columnDesc.isComputedColumn()) {
            result.addAll(ComputedColumnUtil.getCCUsedColsWithProject(project, columnDesc));
        } else {
            result.add(String.format(Locale.ROOT, IDENTIFIER_FORMAT, tableDesc.getIdentity(), columnDesc.getName()));
        }
        return result;
    }

    private Map<String, Set<String>> getAuthorizedTableColumns(List<AclTCR> all) {
        Map<String, Set<String>> authorizedCoarse = Maps.newHashMap();
        all.forEach(aclTCR -> aclTCR.getTable().forEach((dbTblName, columnRow) -> {
            if (authorizedCoarse.containsKey(dbTblName) && Objects.isNull(authorizedCoarse.get(dbTblName))) {
                return;
            }
            if (Objects.isNull(columnRow) || Objects.isNull(columnRow.getColumn())) {
                //default table columns were authorized
                authorizedCoarse.put(dbTblName, null);
                return;
            }
            if (Objects.isNull(authorizedCoarse.get(dbTblName))) {
                authorizedCoarse.put(dbTblName, Sets.newHashSet());
            }
            authorizedCoarse.get(dbTblName).addAll(columnRow.getColumn());
        }));
        return authorizedCoarse;
    }

    public SortedMap<String, AclTCR.Table> getDbAclTable(String project, AclTCR authorized) {
        if (Objects.isNull(authorized)) {
            return Maps.newTreeMap();
        }
        if (Objects.isNull(authorized.getTable())) {
            return getAllDbAclTable(project);
        }
        SortedMap<String, AclTCR.Table> db2AclTable = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        authorized.getTable().forEach((dbTblName, cr) -> {
            TableDesc tableDesc = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getTableDesc(dbTblName);
            if (!db2AclTable.containsKey(tableDesc.getDatabase())) {
                db2AclTable.put(tableDesc.getDatabase(), new AclTCR.Table());
            }

            if (Objects.isNull(cr) || Objects.isNull(cr.getColumn())) {
                AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
                AclTCR.Column aclColumn = new AclTCR.Column();
                aclColumn.addAll(getTableColumns(tableDesc));
                columnRow.setColumn(aclColumn);
                if (Objects.nonNull(cr)) {
                    columnRow.setRow(cr.getRow());
                    columnRow.setLikeRow(cr.getLikeRow());
                    columnRow.setRowFilter(cr.getRowFilter());
                }
                db2AclTable.get(tableDesc.getDatabase()).put(tableDesc.getName(), columnRow);
            } else {
                db2AclTable.get(tableDesc.getDatabase()).put(tableDesc.getName(), cr);
            }
        });
        return db2AclTable;
    }

    private List<String> getTableColumns(TableDesc t) {
        return Optional.ofNullable(t.getColumns()).map(Arrays::stream).orElseGet(Stream::empty).map(ColumnDesc::getName)
                .collect(Collectors.toList());
    }

    public SortedMap<String, AclTCR.Table> getAllDbAclTable(String project) {
        SortedMap<String, AclTCR.Table> db2AclTable = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project).listAllTables().stream()
                .filter(t -> org.apache.commons.lang3.StringUtils.isNotEmpty(t.getDatabase())
                        && org.apache.commons.lang3.StringUtils.isNotEmpty(t.getName()))
                .forEach(t -> {
                    AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
                    AclTCR.Column aclColumn = new AclTCR.Column();
                    aclColumn.addAll(getTableColumns(t));
                    columnRow.setColumn(aclColumn);
                    if (!db2AclTable.containsKey(t.getDatabase())) {
                        db2AclTable.put(t.getDatabase(), new AclTCR.Table());
                    }
                    db2AclTable.get(t.getDatabase()).put(t.getName(), columnRow);
                });
        return db2AclTable;
    }

    public AclTCRDigest getAuthTablesAndColumns(String project, String sid, boolean principal) {
        Set<String> tables = Sets.newHashSet();
        Set<String> columns = Sets.newHashSet();
        val authorized = getDbAclTable(project, getAclTCR(sid, principal));
        for (val database : authorized.entrySet()) {
            for (val table : database.getValue().entrySet()) {
                tables.add(database.getKey() + "." + table.getKey());
                for (val column : table.getValue().getColumn()) {
                    columns.add(database.getKey() + "." + table.getKey() + "." + column);
                }
            }
        }
        AclTCRDigest aclTCRDigest = new AclTCRDigest();
        aclTCRDigest.setTables(tables);
        aclTCRDigest.setColumns(columns);
        return aclTCRDigest;
    }

    public boolean isAllTablesAuthorized(String username, Set<String> groups) {
        final List<AclTCR> all = getAclTCRs(username, groups);
        return isTablesAuthorized(all);
    }

    public SensitiveDataMaskInfo getSensitiveDataMaskInfo(String username, Set<String> groups) {
        SensitiveDataMaskInfo maskInfo = new SensitiveDataMaskInfo();
        List<AclTCR> aclTCRS = getAclTCRs(username, groups);
        for (AclTCR aclTCR : aclTCRS) {
            if (aclTCR.getTable() != null) {
                for (Map.Entry<String, AclTCR.ColumnRow> entry : aclTCR.getTable().entrySet()) {
                    String dbTableName = entry.getKey();
                    AclTCR.ColumnRow columnRow = entry.getValue();
                    if (columnRow != null && columnRow.getColumnSensitiveDataMask() != null) {
                        int sepIdx = dbTableName.indexOf('.');
                        assert sepIdx > -1;
                        String dbName = dbTableName.substring(0, sepIdx);
                        String tableName = dbTableName.substring(sepIdx + 1);
                        maskInfo.addMasks(dbName, tableName, entry.getValue().getColumnSensitiveDataMask());
                    }
                }
            }
        }
        return maskInfo;
    }

    public DependentColumnInfo getDependentColumns(String username, Set<String> groups) {
        DependentColumnInfo info = new DependentColumnInfo();
        List<AclTCR> aclTCRS = getAclTCRs(username, groups);
        for (AclTCR aclTCR : aclTCRS) {
            if (aclTCR.getTable() != null) {
                for (Map.Entry<String, AclTCR.ColumnRow> entry : aclTCR.getTable().entrySet()) {
                    String dbTableName = entry.getKey();
                    AclTCR.ColumnRow columnRow = entry.getValue();
                    if (columnRow != null && columnRow.getDependentColumns() != null) {
                        int sepIdx = dbTableName.indexOf('.');
                        assert sepIdx > -1;
                        String dbName = dbTableName.substring(0, sepIdx);
                        String tableName = dbTableName.substring(sepIdx + 1);
                        info.add(dbName, tableName, entry.getValue().getDependentColumns());
                    }
                }
            }
        }
        info.validate();
        return info;
    }
}

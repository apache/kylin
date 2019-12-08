/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DataModel extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(DataModel.class);
    public static int DIMENSION_ID_BASE = 0;
    public static final int MEASURE_ID_BASE = 100000;

    public enum TableKind implements Serializable {
        FACT, LOOKUP
    }

    public enum RealizationCapacity implements Serializable {
        SMALL, MEDIUM, LARGE
    }

    private KylinConfig config;

    private String alias;

    private String owner;

    private String configLastModifier;

    private long configLastModified;

    private boolean isDraft;

    private String description;

    private String rootFactTableName;

    private String rootFactTableAlias;

//    @JsonProperty("management_type")
//    private ManagementType managementType = ManagementType.TABLE_ORIENTED;

    private List<JoinTableDesc> joinTables;

    private String filterCondition;

    private PartitionDesc partitionDesc;

    private RealizationCapacity capacity = RealizationCapacity.MEDIUM;

    // computed attributes
    private List<NamedColumn> allNamedColumns = new ArrayList<>(); // including deleted ones
    private List<Measure> allMeasures = new ArrayList<>(); // including deleted ones
    private List<ColumnCorrelation> colCorrs = new ArrayList<>();

    private List<String> mpColStrs = Lists.newArrayList();

    private boolean handledAfterBroken = false;

    // computed fields below
    private String project;

    private ImmutableBiMap<Integer, TblColRef> effectiveCols; // excluding DELETED cols

    private ImmutableBiMap<Integer, TblColRef> effectiveDimensions; // including DIMENSION cols

    private ImmutableBiMap<Integer, Measure> effectiveMeasures; // excluding DELETED cols

    private ImmutableMultimap<TblColRef, TblColRef> fk2Pk;

    private List<TblColRef> mpCols;

    private TableRef rootFactTableRef;

    private Set<TableRef> factTableRefs = Sets.newLinkedHashSet();

    private Set<TableRef> lookupTableRefs = Sets.newLinkedHashSet();

    private Set<TableRef> allTableRefs = Sets.newLinkedHashSet();

    private Map<String, TableRef> aliasMap = Maps.newHashMap(); // alias => TableRef, a table has exactly one alias

    private Map<String, TableRef> tableNameMap = Maps.newHashMap(); // name => TableRef, a table maybe referenced by multiple names

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = new ArrayList<>();

    public enum ColumnStatus {
        TOMB, EXIST, DIMENSION
    }

    public enum BrokenReason {
        SCHEMA, NULL, EVENT
    }


    public static class NamedColumn implements Serializable {
        private int id;
        private String name;
        private String aliasDotColumn;
        private ColumnStatus status = ColumnStatus.EXIST;

        public boolean isExist() {
            return status != ColumnStatus.TOMB;
        }
        public boolean isDimension() {
            return status == ColumnStatus.DIMENSION;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAliasDotColumn() {
            return aliasDotColumn;
        }

        public void setAliasDotColumn(String aliasDotColumn) {
            this.aliasDotColumn = aliasDotColumn;
        }

        public ColumnStatus getStatus() {
            return status;
        }

        public void setStatus(ColumnStatus status) {
            this.status = status;
        }
    }

    public static class Measure extends MeasureDesc {
        private int id;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }

    public static class ColumnCorrelation implements Serializable {
        @JsonProperty("name")
        public String name;
        @JsonProperty("correlation_type") // "hierarchy" or "joint"
        public String corrType;
        @JsonProperty("columns")
        public String[] aliasDotColumns;

        public TblColRef[] cols;

    }

    // ============================================================================

    // don't use unless you're sure(when in doubt, leave it out), for jackson only
    public DataModel() {
        super();
    }

    public DataModel(KylinConfig config) {
        this.config = config;
    }

    public KylinConfig getConfig() {
        return config;
    }

    @Override
    public String resourceName() {
        return uuid;
    }

//    public ManagementType getManagementType() {
//        return managementType;
//    }

//    public void setManagementType(ManagementType managementType) {
//        this.managementType = managementType;
//    }

    public String getRootFactTableName() {
        return rootFactTableName;
    }

    public void setRootFactTableName(String rootFactTableName) {
        this.rootFactTableName = rootFactTableName;
    }

    public TableRef getRootFactTable() {
        return rootFactTableRef;
    }

    public void setRootFactTable(TableRef rootFactTableRef) {
        this.rootFactTableRef = rootFactTableRef;
    }

    public Set<TableRef> getAllTables() {
        return allTableRefs;
    }

    public void setAllTables(Set<TableRef> allTables) {
        this.allTableRefs = allTables;
    }
    public Set<TableRef> getFactTables() {
        return factTableRefs;
    }

    public Map<String, TableRef> getAliasMap() {
        return Collections.unmodifiableMap(aliasMap);
    }

    public void setAliasMap(Map<String, TableRef> aliasMap) {
        this.aliasMap = aliasMap;
    }

    public Set<TableRef> getLookupTables() {
        return lookupTableRefs;
    }

    public List<JoinTableDesc> getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(List<JoinTableDesc> joinTables) {
        this.joinTables = joinTables;
    }

    public PartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public void setPartitionDesc(PartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    //    public JoinDesc getJoinByPKSide(TableRef table) {
//        return joinsGraph.getJoinByPKSide(table);
//    }
//
//    public JoinsGraph getJoinsGraph() {
//        return joinsGraph;
//    }

//    public DataCheckDesc getDataCheckDesc() {
//        if (dataCheckDesc == null) {
//            return new DataCheckDesc();
//        }
//        return dataCheckDesc;
//    }
//
//    public void setDataCheckDesc(DataCheckDesc dataCheckDesc) {
//        this.dataCheckDesc = dataCheckDesc;
//    }

    public boolean isLookupTable(TableRef t) {
        if (t == null)
            return false;
        else
            return lookupTableRefs.contains(t);
    }

    public boolean isJoinTable(String fullTableName) {
        if (joinTables == null) {
            return false;
        }
        for (JoinTableDesc table : joinTables) {
            if (table.getTable().equals(fullTableName)) {
                return true;
            }
        }
        return false;
    }

    public boolean isLookupTable(String fullTableName) {
        for (TableRef t : lookupTableRefs) {
            if (t.getTableIdentity().equals(fullTableName))
                return true;
        }
        return false;
    }

    public boolean isFactTable(TableRef t) {
        if (t == null)
            return false;
        else
            return factTableRefs.contains(t);
    }

    //TODO: different from isFactTable(TableRef t)
    public boolean isFactTable(String fullTableName) {
        for (TableRef t : factTableRefs) {
            if (t.getTableIdentity().equals(fullTableName))
                return true;
        }
        return false;
    }

    public boolean isRootFactTable(TableDesc table) {
        if (table == null || StringUtils.isBlank(table.getIdentity()) || StringUtils.isBlank(table.getProject())) {
            return false;
        }

        return rootFactTableRef.getTableIdentity().equals(table.getIdentity())
                && rootFactTableRef.getTableDesc().getProject().equals(table.getProject());
    }

    public boolean containsTable(TableDesc table) {
        if (table == null)
            return false;

        for (TableRef t : allTableRefs) {
            if (t.getTableIdentity().equals(table.getIdentity())
                    && StringUtil.equals(t.getTableDesc().getProject(), table.getProject()))
                return true;
        }
        return false;
    }

    public TblColRef findColumn(String table, String column) throws IllegalArgumentException {
        TableRef tableRef = findTable(table);
        TblColRef result = tableRef.getColumn(column.toUpperCase(Locale.ROOT));
        if (result == null)
            throw new IllegalArgumentException("Column not found by " + table + "." + column);
        return result;
    }

    public TblColRef findColumn(String column) throws IllegalArgumentException {
        TblColRef result = null;
        String input = column;

        column = column.toUpperCase(Locale.ROOT);
        int cut = column.lastIndexOf('.');
        if (cut > 0) {
            // table specified
            result = findColumn(column.substring(0, cut), column.substring(cut + 1));
        } else {
            // table not specified, try each table
            for (TableRef tableRef : allTableRefs) {
                result = tableRef.getColumn(column);
                if (result != null)
                    break;
            }
        }

        if (result == null)
            throw new IllegalArgumentException("Column not found by " + input);

        return result;
    }

    // find by unique name, that must uniquely identifies a table in the model
    public TableRef findTable(String table) throws IllegalArgumentException {
        TableRef result = tableNameMap.get(table.toUpperCase());
        if (result == null) {
            int endOfDatabaseName = table.indexOf(".");
            if (endOfDatabaseName > -1) {
                result = tableNameMap.get(table.substring(endOfDatabaseName + 1));
            }
            if (result == null) {
                throw new IllegalArgumentException("Table not found by " + table);
            }
        }
        return result;
    }

    // find by table identity, that may match multiple tables in the model
    public TableRef findFirstTable(String tableIdentity) throws IllegalArgumentException {
        if (rootFactTableRef.getTableIdentity().equals(tableIdentity))
            return rootFactTableRef;

        for (TableRef fact : factTableRefs) {
            if (fact.getTableIdentity().equals(tableIdentity))
                return fact;
        }

        for (TableRef lookup : lookupTableRefs) {
            if (lookup.getTableIdentity().equals(tableIdentity))
                return lookup;
        }
        throw new IllegalArgumentException("Table not found by " + tableIdentity + " in model " + uuid);
    }

    public void init() {
        this.effectiveCols = initAllNamedColumns(NamedColumn::isExist);
        this.effectiveDimensions = initAllNamedColumns(NamedColumn::isDimension);
        initAllMeasures();
    }

    //Check if the filter condition is illegal.
    private void initFilterCondition() {
        if (null == this.filterCondition) {
            return;
        }
        int quotationType = 0;
        int len = this.filterCondition.length();
        for (int i = 0; i < len; i++) {
            //If a ';' which is not within a string is found, throw exception.
            if (';' == this.filterCondition.charAt(i) && 0 == quotationType) {
                throw new IllegalStateException(
                        "Filter Condition is Illegal. Please check it and make sure it's an appropriate expression for WHERE clause");
            }
            if ('\'' == this.filterCondition.charAt(i)) {
                if (quotationType > 0) {
                    if (1 == quotationType) {
                        quotationType = 0;
                        continue;
                    }
                } else {
                    if (0 == quotationType) {
                        quotationType = 1;
                        continue;
                    }
                }
            }
            if ('"' == this.filterCondition.charAt(i)) {
                if (quotationType > 0) {
                    if (2 == quotationType) {
                        quotationType = 0;
                        continue;
                    }
                } else {
                    if (0 == quotationType) {
                        quotationType = 2;
                        continue;
                    }
                }
            }
        }
    }

    /**
     * Add error info and thrown exception out
     */
    public void addError(String message) {
        addError(message, false);
    }

    /**
     * @param message error message
     * @param silent  if throw exception
     */
    public void addError(String message, boolean silent) {
        if (!silent) {
            throw new IllegalStateException(message);
        } else {
            this.errors.add(message);
        }
    }

    public List<String> getError() {
        return this.errors;
    }

    @Override
    public String toString() {
        return "DataModel [" + getAlias() + "]";
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getProject() {
        return project;
    }


    private ImmutableBiMap<Integer, TblColRef> initAllNamedColumns(Predicate<NamedColumn> filter) {
        List<TblColRef> all = new ArrayList<>(allNamedColumns.size());
        ImmutableBiMap.Builder<Integer, TblColRef> mapBuilder = ImmutableBiMap.builder();
        for (NamedColumn d : allNamedColumns) {
            if (!d.isExist()) {
                continue;
            }
            TblColRef col = this.findColumn(d.aliasDotColumn);
            d.aliasDotColumn = col.getIdentity();
            all.add(col);

            if (filter.test(d)) {
                mapBuilder.put(d.id, col);
            }
        }

        ImmutableBiMap<Integer, TblColRef> cols = mapBuilder.build();
        checkNoDup(cols);
        return cols;
    }

    private <T> void checkNoDup(ImmutableBiMap<Integer, T> idMap) {
        Map<T, Integer> reverseMap = new HashMap<>();
        for (Map.Entry<Integer, T> e : idMap.entrySet()) {
            int id = e.getKey();
            T value = e.getValue();
            if (reverseMap.containsKey(value)) {
                throw new IllegalStateException(String.format("Illegal model '%d', %s has duplicated ID: %s and %d", id,
                        value, reverseMap.get(value), id));
            }
            reverseMap.put(value, id);
        }
    }

    private void initAllMeasures() {
        ImmutableBiMap.Builder<Integer, Measure> mapBuilder = ImmutableBiMap.builder();
        for (Measure m : allMeasures) {
            try {
                m.setName(m.getName().toUpperCase());
                mapBuilder.put(m.id, m);
//                if (!m.tomb) {
//                    mapBuilder.put(m.id, m);
//                    FunctionDesc func = m.getFunction();
//                    func.init(this);
//                }
            } catch (Exception e) {
                throw new IllegalStateException("Cannot init measure " + m.getName() + ": " + e.getMessage(), e);
            }
        }

        this.effectiveMeasures = mapBuilder.build();
        checkNoDupAndEffective(effectiveMeasures);
    }

    private void initFk2Pk() {
        ImmutableMultimap.Builder<TblColRef, TblColRef> builder = ImmutableMultimap.builder();
        for (JoinTableDesc joinTable : this.getJoinTables()) {
            JoinDesc join = joinTable.getJoin();
            int n = join.getForeignKeyColumns().length;
            for (int i = 0; i < n; i++) {
                TblColRef pk = join.getPrimaryKeyColumns()[i];
                TblColRef fk = join.getForeignKeyColumns()[i];
                builder.put(fk, pk);
            }
        }
        this.fk2Pk = builder.build();
    }

    private void checkNoDupAndEffective(ImmutableBiMap<Integer, Measure> effectiveMeasures) {
        checkNoDup(effectiveMeasures);

        // check there is one count()
        int countNum = 0;
        for (MeasureDesc m : effectiveMeasures.values()) {
            if (m.getFunction().isCountConstant())
                countNum++;
        }
        if (countNum != 1)
            throw new IllegalStateException(
                    String.format("Illegal model '%s', should have one and only one COUNT() measure but there are %d",
                            uuid, countNum));

        // check all measure columns are effective
        for (MeasureDesc m : effectiveMeasures.values()) {
            List<TblColRef> mCols = m.getFunction().getColRefs();
            if (effectiveCols.values().containsAll(mCols) == false) {
                List<TblColRef> notEffective = new ArrayList<>(mCols);
                notEffective.removeAll(effectiveCols.values());
                throw new IllegalStateException(
                        String.format("Illegal model '%s', some columns referenced in %s is not on model: %s", uuid, m,
                                notEffective));
            }
        }
    }

    public List<Measure> getAllMeasures() {
        return allMeasures;
    }

    /**
     * returns ID <==> TblColRef
     */
    public ImmutableBiMap<Integer, TblColRef> getEffectiveColsMap() {
        return effectiveCols;
    }

    public ImmutableBiMap<Integer, TblColRef> getEffectiveDimenionsMap() {
        return effectiveDimensions;
    }

    /**
     * returns ID <==> Measure
     */
    public ImmutableBiMap<Integer, Measure> getEffectiveMeasureMap() {
        return effectiveMeasures;
    }

    //TODO: !!! check the returned
    public @Nullable
    Integer getColId(TblColRef colRef) {
        return effectiveCols.inverse().get(colRef);
    }

    public TblColRef getColRef(Integer colId) {
        return effectiveCols.get(colId);
    }

    public boolean isExtendedColumn(TblColRef tblColRef) {
        return false; // TODO: enable derived
    }

    private void checkMPColsBelongToModel(List<TblColRef> tcr) {
        Set<TblColRef> refSet = effectiveCols.values();
        if (!refSet.containsAll(Sets.newHashSet(tcr))) {
            throw new IllegalStateException("Primary partition column should inside of this model.");
        }
    }

    public String getAlias() {
        if (StringUtils.isEmpty(this.alias)) {
            return this.uuid;
        }
        return this.alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<NamedColumn> getAllNamedColumns() {
        return allNamedColumns;
    }

    public void setAllNamedColumns(List<NamedColumn> allNamedColumns) {
        this.allNamedColumns = allNamedColumns;
    }

    public void setAllMeasures(List<Measure> allMeasures) {
        this.allMeasures = allMeasures;
    }

    public List<ColumnCorrelation> getColCorrs() {
        return colCorrs;
    }

    public ImmutableMultimap<TblColRef, TblColRef> getFk2Pk() {
        return fk2Pk;
    }

    public int getColumnIdByColumnName(String aliasDotName) {
        Preconditions.checkArgument(allNamedColumns != null);
        for (NamedColumn col : allNamedColumns) {
            if (col.aliasDotColumn.equalsIgnoreCase(aliasDotName))
                return col.id;
        }
        return -1;
    }

    public String getColumnNameByColumnId(int id) {
        Preconditions.checkArgument(allNamedColumns != null);
        for (NamedColumn col : allNamedColumns) {
            if (col.id == id)
                return col.aliasDotColumn;
        }
        return null;
    }

    public String getNameByColumnId(int id) {
        Preconditions.checkArgument(allNamedColumns != null);
        for (NamedColumn col : allNamedColumns) {
            if (col.id == id)
                return col.name;
        }
        return null;
    }

}

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

package org.apache.kylin.metadata.model;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.JoinsTree.Chain;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DataModelDesc extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(DataModelDesc.class);

    public static enum TableKind implements Serializable {
        FACT, LOOKUP
    }

    public static enum RealizationCapacity implements Serializable {
        SMALL, MEDIUM, LARGE
    }

    private KylinConfig config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("is_draft")
    private boolean isDraft;

    @JsonProperty("description")
    private String description;

    @JsonProperty("fact_table")
    private String rootFactTable;

    @JsonProperty("lookups")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private JoinTableDesc[] joinTables;

    @JsonProperty("join_tables")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private JoinTableDesc[] deprecatedLookups; // replaced by "join_tables" since KYLIN-1875

    @JsonProperty("dimensions")
    private List<ModelDimensionDesc> dimensions;

    @JsonProperty("metrics")
    private String[] metrics;

    @JsonProperty("filter_condition")
    private String filterCondition;

    @JsonProperty("partition_desc")
    PartitionDesc partitionDesc;

    @JsonProperty("capacity")
    private RealizationCapacity capacity = RealizationCapacity.MEDIUM;

    @JsonProperty("computed_columns")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private List<ComputedColumnDesc> computedColumnDescs = Lists.newArrayList();

    // computed attributes
    private TableRef rootFactTableRef;
    private Set<TableRef> factTableRefs = Sets.newLinkedHashSet();
    private Set<TableRef> lookupTableRefs = Sets.newLinkedHashSet();
    private Set<TableRef> allTableRefs = Sets.newLinkedHashSet();
    private Map<String, TableRef> aliasMap = Maps.newHashMap(); // alias => TableRef, a table has exactly one alias
    private Map<String, TableRef> tableNameMap = Maps.newHashMap(); // name => TableRef, a table maybe referenced by multiple names
    private JoinsTree joinsTree;

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = new ArrayList<String>();

    // don't use unless you're sure, for jackson only
    public DataModelDesc() {
    }

    public KylinConfig getConfig() {
        return config;
    }

    public String getName() {
        return name;
    }

    // for updating name from draft to ready
    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public boolean isDraft() {
        return isDraft;
    }

    public void setDraft(boolean isDraft) {
        this.isDraft = isDraft;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public TableRef getRootFactTable() {
        return rootFactTableRef;
    }

    public String getRootFactTableName() {
        return rootFactTable;
    }

    public void setRootFactTableName(String rootFactTable) {
        this.rootFactTable = rootFactTable;
    }

    public Set<TableRef> getAllTables() {
        return allTableRefs;
    }

    public Set<TableRef> getFactTables() {
        return factTableRefs;
    }

    public Map<String, TableRef> getAliasMap() {
        return Collections.unmodifiableMap(aliasMap);
    }

    public Set<TableRef> getLookupTables() {
        return lookupTableRefs;
    }

    public JoinTableDesc[] getJoinTables() {
        return joinTables;
    }

    public void setJoinTables(JoinTableDesc[] joinTables) {
        this.joinTables = joinTables;
    }

    public JoinDesc getJoinByPKSide(TableRef table) {
        return joinsTree.getJoinByPKSide(table);
    }

    public JoinsTree getJoinsTree() {
        return joinsTree;
    }

    @Deprecated
    public List<TableDesc> getLookupTableDescs() {
        List<TableDesc> result = Lists.newArrayList();
        for (TableRef table : getLookupTables()) {
            result.add(table.getTableDesc());
        }
        return result;
    }

    public boolean isLookupTable(TableRef t) {
        if (t == null)
            return false;
        else
            return lookupTableRefs.contains(t);
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

    public boolean containsTable(TableDesc table) {
        for (TableRef t : allTableRefs) {
            if (t.getTableIdentity().equals(table.getIdentity())
                    && StringUtil.equals(t.getTableDesc().getProject(), table.getProject()))
                return true;
        }
        return false;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    public PartitionDesc getPartitionDesc() {
        return partitionDesc;
    }

    public void setPartitionDesc(PartitionDesc partitionDesc) {
        this.partitionDesc = partitionDesc;
    }

    public RealizationCapacity getCapacity() {
        return capacity;
    }

    public void setCapacity(RealizationCapacity capacity) {
        this.capacity = capacity;
    }

    public TblColRef findColumn(String table, String column) throws IllegalArgumentException {
        TableRef tableRef = findTable(table);
        TblColRef result = tableRef.getColumn(column.toUpperCase());
        if (result == null)
            throw new IllegalArgumentException("Column not found by " + table + "." + column);
        return result;
    }

    public TblColRef findColumn(String column) throws IllegalArgumentException {
        TblColRef result = null;
        String input = column;

        column = column.toUpperCase();
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
            throw new IllegalArgumentException("Table not found by " + table);
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
        throw new IllegalArgumentException("Table not found by " + tableIdentity + " in model " + name);
    }

    public void init(KylinConfig config, Map<String, TableDesc> originalTables, List<DataModelDesc> dataModelDescs) {
        //tweak the tables according to Computed Columns defined in model
        Map<String, TableDesc> tables = Maps.newHashMap();
        for (Map.Entry<String, TableDesc> entry : originalTables.entrySet()) {
            String s = entry.getKey();
            TableDesc tableDesc = entry.getValue();
            TableDesc extendedTableDesc = tableDesc.appendColumns(createComputedColumns(tableDesc));
            tables.put(s, extendedTableDesc);
        }

        this.config = config;

        initJoinTablesForUpgrade();
        initTableAlias(tables);
        initJoinColumns();
        reorderJoins(tables);
        initJoinsTree();
        initDimensionsAndMetrics();
        initPartitionDesc();
        initComputedColumns(dataModelDescs);
        initFilterCondition();

        boolean reinit = validate();
        if (reinit) { // model slightly changed by validate() and must init() again
            init(config, tables, dataModelDescs);
        }
    }

    private ColumnDesc[] createComputedColumns(final TableDesc tableDesc) {
        final MutableInt id = new MutableInt(tableDesc.getColumnCount());
        return FluentIterable.from(this.computedColumnDescs).filter(new Predicate<ComputedColumnDesc>() {
            @Override
            public boolean apply(@Nullable ComputedColumnDesc input) {
                return tableDesc.getIdentity().equalsIgnoreCase(input.getTableIdentity());
            }
        }).transform(new Function<ComputedColumnDesc, ColumnDesc>() {
            @Nullable
            @Override
            public ColumnDesc apply(@Nullable ComputedColumnDesc input) {
                id.increment();
                ColumnDesc columnDesc = new ColumnDesc(id.toString(), input.getColumnName(), input.getDatatype(),
                        input.getComment(), null, null, input.getExpression());
                return columnDesc;
            }
        }).toArray(ColumnDesc.class);
    }

    private void initJoinTablesForUpgrade() {
        if (joinTables == null) {
            joinTables = new JoinTableDesc[0];
        }
        if (deprecatedLookups != null) {
            JoinTableDesc[] copy = Arrays.copyOf(joinTables, joinTables.length + deprecatedLookups.length);
            System.arraycopy(deprecatedLookups, 0, copy, joinTables.length, deprecatedLookups.length);
            joinTables = copy;
            deprecatedLookups = null;
        }
    }

    private void initTableAlias(Map<String, TableDesc> tables) {
        factTableRefs.clear();
        lookupTableRefs.clear();
        allTableRefs.clear();
        aliasMap.clear();
        tableNameMap.clear();

        if (StringUtils.isEmpty(rootFactTable)) {
            throw new IllegalStateException("root fact table should not be empty");
        }

        rootFactTable = rootFactTable.toUpperCase();
        if (tables.containsKey(rootFactTable) == false)
            throw new IllegalStateException("Root fact table does not exist:" + rootFactTable);

        TableDesc rootDesc = tables.get(rootFactTable);
        rootFactTableRef = new TableRef(this, rootDesc.getName(), rootDesc);

        addAlias(rootFactTableRef);
        factTableRefs.add(rootFactTableRef);

        for (JoinTableDesc join : joinTables) {
            join.setTable(join.getTable().toUpperCase());

            if (tables.containsKey(join.getTable()) == false)
                throw new IllegalStateException("Join table does not exist:" + join.getTable());

            TableDesc tableDesc = tables.get(join.getTable());
            String alias = join.getAlias();
            if (alias == null) {
                alias = tableDesc.getName();
            }
            alias = alias.toUpperCase();
            join.setAlias(alias);

            TableRef ref = new TableRef(this, alias, tableDesc);

            join.setTableRef(ref);
            addAlias(ref);
            (join.getKind() == TableKind.LOOKUP ? lookupTableRefs : factTableRefs).add(ref);
        }

        tableNameMap.putAll(aliasMap);
        allTableRefs.addAll(factTableRefs);
        allTableRefs.addAll(lookupTableRefs);
    }

    private void addAlias(TableRef ref) {
        String alias = ref.getAlias();
        if (aliasMap.containsKey(alias))
            throw new IllegalStateException("Alias '" + alias + "' ref to multiple tables: " + ref.getTableIdentity()
                    + ", " + aliasMap.get(alias).getTableIdentity());
        aliasMap.put(alias, ref);

        TableDesc table = ref.getTableDesc();
        addTableName(table.getName(), ref);
        addTableName(table.getIdentity(), ref);
    }

    private void addTableName(String name, TableRef ref) {
        if (tableNameMap.containsKey(name)) {
            tableNameMap.put(name, null); // conflict name
        } else {
            tableNameMap.put(name, ref);
        }
    }

    private void initDimensionsAndMetrics() {
        for (ModelDimensionDesc dim : dimensions) {
            dim.init(this);
        }
        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = findColumn(metrics[i]).getIdentity();
        }
    }

    private void initPartitionDesc() {
        if (this.partitionDesc != null)
            this.partitionDesc.init(this);
    }

    private void initComputedColumns(List<DataModelDesc> allDataModelDescs) {
        Preconditions.checkNotNull(allDataModelDescs);

        List<Pair<ComputedColumnDesc, DataModelDesc>> existingCCs = Lists.newArrayList();

        for (DataModelDesc dataModelDesc : allDataModelDescs) {
            if (!StringUtils.equals(dataModelDesc.getName(), this.getName())) {
                for (ComputedColumnDesc cc : dataModelDesc.getComputedColumnDescs()) {
                    existingCCs.add(Pair.newPair(cc, dataModelDesc));
                }
            }
        }

        for (ComputedColumnDesc newCC : this.computedColumnDescs) {

            newCC.init(aliasMap, rootFactTableRef.getAlias());
            final String newCCFullName = newCC.getFullName();
            final String newCCColumnName = newCC.getColumnName();

            for (Pair<ComputedColumnDesc, DataModelDesc> pair : existingCCs) {
                DataModelDesc dataModelDesc = pair.getSecond();
                ComputedColumnDesc cc = pair.getFirst();

                if (StringUtils.equalsIgnoreCase(cc.getFullName(), newCCFullName) && !(cc.equals(newCC))) {
                    throw new IllegalArgumentException(String.format(
                            "Column name for computed column %s is already used in model %s, you should apply the same expression ' %s ' here, or use a different column name.",
                            newCCFullName, dataModelDesc.getName(), cc.getExpression()));
                }

                if (isTwoCCDefinitionEquals(cc.getExpression(), newCC.getExpression())
                        && !StringUtils.equalsIgnoreCase(cc.getColumnName(), newCCColumnName)) {
                    throw new IllegalArgumentException(String.format(
                            "Expression %s in computed column %s is already defined by computed column %s from model %s, you should use the same column name: ' %s ' .",
                            newCC.getExpression(), newCCFullName, cc.getFullName(), dataModelDesc.getName(),
                            cc.getColumnName()));
                }
            }
            existingCCs.add(Pair.newPair(newCC, this));
        }
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

    private boolean isTwoCCDefinitionEquals(String definition0, String definition1) {
        definition0 = definition0.replaceAll("\\s*", "");
        definition1 = definition1.replaceAll("\\s*", "");
        return definition0.equalsIgnoreCase(definition1);
    }

    private void initJoinColumns() {

        for (JoinTableDesc joinTable : joinTables) {
            TableRef dimTable = joinTable.getTableRef();
            JoinDesc join = joinTable.getJoin();
            if (join == null)
                throw new IllegalStateException("Missing join conditions on table " + dimTable);

            StringUtil.toUpperCaseArray(join.getForeignKey(), join.getForeignKey());
            StringUtil.toUpperCaseArray(join.getPrimaryKey(), join.getPrimaryKey());

            // primary key
            String[] pks = join.getPrimaryKey();
            TblColRef[] pkCols = new TblColRef[pks.length];
            for (int i = 0; i < pks.length; i++) {
                TblColRef col = dimTable.getColumn(pks[i]);
                if (col == null) {
                    col = findColumn(pks[i]);
                }
                if (col == null || col.getTableRef().equals(dimTable) == false) {
                    throw new IllegalStateException("Can't find PK column " + pks[i] + " in table " + dimTable);
                }
                pks[i] = col.getIdentity();
                pkCols[i] = col;
            }
            join.setPrimaryKeyColumns(pkCols);

            // foreign key
            String[] fks = join.getForeignKey();
            TblColRef[] fkCols = new TblColRef[fks.length];
            for (int i = 0; i < fks.length; i++) {
                TblColRef col = findColumn(fks[i]);
                if (col == null) {
                    throw new IllegalStateException("Can't find FK column " + fks[i]);
                }
                fks[i] = col.getIdentity();
                fkCols[i] = col;
            }
            join.setForeignKeyColumns(fkCols);

            join.sortByFK();

            // Validate join in dimension
            TableRef fkTable = fkCols[0].getTableRef();
            if (pkCols.length == 0 || fkCols.length == 0)
                throw new IllegalStateException("Missing join columns on table " + dimTable);
            if (pkCols.length != fkCols.length) {
                throw new IllegalStateException("Primary keys(" + dimTable + ")" + Arrays.toString(pks)
                        + " are not consistent with Foreign keys(" + fkTable + ") " + Arrays.toString(fks));
            }
            for (int i = 0; i < fkCols.length; i++) {
                if (!fkCols[i].getDatatype().equals(pkCols[i].getDatatype())) {
                    logger.warn("PK " + dimTable + "." + pkCols[i].getName() + "." + pkCols[i].getDatatype()
                            + " are not consistent with FK " + fkTable + "." + fkCols[i].getName() + "."
                            + fkCols[i].getDatatype());
                }
            }
        }
    }

    private void initJoinsTree() {
        List<JoinDesc> joins = new ArrayList<>();
        for (JoinTableDesc joinTable : joinTables) {
            joins.add(joinTable.getJoin());
        }
        joinsTree = new JoinsTree(rootFactTableRef, joins);
    }

    private void reorderJoins(Map<String, TableDesc> tables) {
        if (joinTables.length == 0) {
            return;
        }

        Map<String, List<JoinTableDesc>> fkMap = Maps.newHashMap();
        for (JoinTableDesc joinTable : joinTables) {
            JoinDesc join = joinTable.getJoin();
            String fkSideName = join.getFKSide().getAlias();
            if (fkMap.containsKey(fkSideName)) {
                fkMap.get(fkSideName).add(joinTable);
            } else {
                List<JoinTableDesc> joinTableList = Lists.newArrayList();
                joinTableList.add(joinTable);
                fkMap.put(fkSideName, joinTableList);
            }
        }

        JoinTableDesc[] orderedJoinTables = new JoinTableDesc[joinTables.length];
        int orderedIndex = 0;

        Queue<JoinTableDesc> joinTableBuff = new ArrayDeque<JoinTableDesc>();
        TableDesc rootDesc = tables.get(rootFactTable);
        joinTableBuff.addAll(fkMap.get(rootDesc.getName()));
        while (!joinTableBuff.isEmpty()) {
            JoinTableDesc head = joinTableBuff.poll();
            orderedJoinTables[orderedIndex++] = head;
            String headAlias = head.getJoin().getPKSide().getAlias();
            if (fkMap.containsKey(headAlias)) {
                joinTableBuff.addAll(fkMap.get(headAlias));
            }
        }

        joinTables = orderedJoinTables;
    }

    private boolean validate() {

        // ensure no dup between dimensions/metrics
        for (ModelDimensionDesc dim : dimensions) {
            String table = dim.getTable();
            for (String c : dim.getColumns()) {
                TblColRef dcol = findColumn(table, c);
                metrics = ArrayUtils.removeElement(metrics, dcol.getIdentity());
            }
        }

        Set<TblColRef> mcols = new HashSet<>();
        for (String m : metrics) {
            mcols.add(findColumn(m));
        }

        // validate PK/FK are in dimensions
        boolean pkfkDimAmended = false;
        for (Chain chain : joinsTree.tableChains.values()) {
            pkfkDimAmended = validatePkFkDim(chain.join, mcols) || pkfkDimAmended;
        }
        return pkfkDimAmended;
    }

    private boolean validatePkFkDim(JoinDesc join, Set<TblColRef> mcols) {
        if (join == null)
            return false;

        boolean pkfkDimAmended = false;

        for (TblColRef c : join.getForeignKeyColumns()) {
            if (!mcols.contains(c)) {
                pkfkDimAmended = validatePkFkDim(c) || pkfkDimAmended;
            }
        }
        for (TblColRef c : join.getPrimaryKeyColumns()) {
            if (!mcols.contains(c)) {
                pkfkDimAmended = validatePkFkDim(c) || pkfkDimAmended;
            }
        }
        return pkfkDimAmended;
    }

    private boolean validatePkFkDim(TblColRef c) {
        String t = c.getTableAlias();
        ModelDimensionDesc dimDesc = null;
        for (ModelDimensionDesc dim : dimensions) {
            if (dim.getTable().equals(t)) {
                dimDesc = dim;
                break;
            }
        }

        if (dimDesc == null) {
            dimDesc = new ModelDimensionDesc();
            dimDesc.setTable(t);
            dimDesc.setColumns(new String[0]);
            dimensions.add(dimDesc);
        }

        if (ArrayUtils.contains(dimDesc.getColumns(), c.getName()) == false) {
            String[] newCols = ArrayUtils.add(dimDesc.getColumns(), c.getName());
            dimDesc.setColumns(newCols);
            return true;
        }

        return false;
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
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DataModelDesc modelDesc = (DataModelDesc) o;

        if (!name.equals(modelDesc.name))
            return false;
        if (!getRootFactTable().equals(modelDesc.getRootFactTable()))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + name.hashCode();
        result = 31 * result + getRootFactTable().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DataModelDesc [name=" + name + "]";
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String descName) {
        return ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    public List<ModelDimensionDesc> getDimensions() {
        return dimensions;
    }

    public ComputedColumnDesc findCCByCCColumnName(final String columnName) {
        return Iterables.find(this.computedColumnDescs, new Predicate<ComputedColumnDesc>() {
            @Override
            public boolean apply(@Nullable ComputedColumnDesc input) {
                Preconditions.checkNotNull(input);
                return columnName.equals(input.getColumnName());
            }
        });
    }

    public Set<String> getComputedColumnNames() {
        Set<String> ccColumnNames = Sets.newHashSet();
        for (ComputedColumnDesc cc : this.getComputedColumnDescs()) {
            ccColumnNames.add(cc.getColumnName());
        }
        return Collections.unmodifiableSet(ccColumnNames);
    }

    public List<ComputedColumnDesc> getComputedColumnDescs() {
        return computedColumnDescs;
    }

    public void setComputedColumnDescs(List<ComputedColumnDesc> computedColumnDescs) {
        this.computedColumnDescs = computedColumnDescs;
    }

    public String[] getMetrics() {
        return metrics;
    }

    @Deprecated
    public void setDimensions(List<ModelDimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    @Deprecated
    public void setMetrics(String[] metrics) {
        this.metrics = metrics;
    }

    public String getProject() {
        return ProjectManager.getInstance(getConfig()).getProjectOfModel(this.getName()).getName();
    }

    public ProjectInstance getProjectInstance() {
        return ProjectManager.getInstance(getConfig()).getProjectOfModel(this.getName());
    }

    public static DataModelDesc getCopyOf(DataModelDesc orig) {
        DataModelDesc copy = new DataModelDesc();
        copy.config = orig.config;
        copy.name = orig.name;
        copy.isDraft = orig.isDraft;
        copy.owner = orig.owner;
        copy.description = orig.description;
        copy.rootFactTable = orig.rootFactTable;
        copy.joinTables = orig.joinTables;
        copy.dimensions = orig.dimensions;
        copy.metrics = orig.metrics;
        copy.filterCondition = orig.filterCondition;
        copy.partitionDesc = PartitionDesc.getCopyOf(orig.getPartitionDesc());
        copy.capacity = orig.capacity;
        copy.computedColumnDescs = orig.computedColumnDescs;
        copy.updateRandomUuid();
        return copy;
    }
}

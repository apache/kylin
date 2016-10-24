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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DataModelDesc extends RootPersistentEntity {
    private static final Logger logger = LoggerFactory.getLogger(DataModelDesc.class);
    public static enum RealizationCapacity {
        SMALL, MEDIUM, LARGE
    }
    
    private KylinConfig config;

    @JsonProperty("name")
    private String name;

    @JsonProperty("owner")
    private String owner;

    @JsonProperty("description")
    private String description;

    @JsonProperty("fact_table")
    private String factTable;

    @JsonProperty("lookups")
    private LookupDesc[] lookups;

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

    // computed attributes
    private TableRef factTableRef;
    private List<TableRef> lookupTableRefs = Lists.newArrayList();
    private Map<String, TableRef> aliasMap = Maps.newHashMap(); // a table has exactly one alias
    private Map<String, TableRef> tableNameMap = Maps.newHashMap(); // a table maybe referenced by multiple names

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = new ArrayList<String>();

    public KylinConfig getConfig() {
        return config;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Collection<String> getAllTables() {
        HashSet<String> ret = Sets.newHashSet();
        ret.add(factTable);
        for (LookupDesc lookupDesc : lookups)
            ret.add(lookupDesc.getTable());
        return ret;
    }

    public String getFactTable() {
        return factTable;
    }

    public TableRef getFactTableRef() {
        return factTableRef;
    }

    public List<TableRef> getLookupTableRefs() {
        return lookupTableRefs;
    }
    
    @Deprecated
    public List<TableDesc> getLookupTableDescs() {
        List<TableDesc> result = Lists.newArrayList();
        for (TableRef table : getLookupTableRefs()) {
            result.add(table.getTableDesc());
        }
        return result;
    }

    public void setFactTable(String factTable) {
        this.factTable = factTable.toUpperCase();
    }

    public LookupDesc[] getLookups() {
        return lookups;
    }

    public void setLookups(LookupDesc[] lookups) {
        this.lookups = lookups;
    }

    public boolean isFactTable(String factTable) {
        return this.factTable.equalsIgnoreCase(factTable);
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

    public TblColRef findPKByFK(TblColRef fk, String joinType) {
        assert isFactTable(fk.getTable());

        TblColRef candidate = null;

        for (LookupDesc dim : lookups) {
            JoinDesc join = dim.getJoin();
            if (join == null)
                continue;

            if (joinType != null && !joinType.equals(join.getType()))
                continue;

            int find = ArrayUtils.indexOf(join.getForeignKeyColumns(), fk);
            if (find >= 0) {
                candidate = join.getPrimaryKeyColumns()[find];
                if (join.getForeignKeyColumns().length == 1) { // is single
                    // column join?
                    break;
                }
            }
        }
        return candidate;
    }

    public TblColRef findColumn(String table, String column) {
        TableRef tableRef = findTable(table);
        TblColRef result = tableRef.getColumn(column);
        if (result == null)
            throw new IllegalArgumentException("Column not found by " + table + "." + column);
        return result;
    }
    
    public TblColRef findColumn(String column) {
        TblColRef result = null;

        int cut = column.lastIndexOf('.');
        if (cut > 0) {
            // table specified
            result = findColumn(column.substring(0, cut), column.substring(cut + 1));
        } else {
            // table not specified, try each table
            result = factTableRef.getColumn(column);
            if (result == null) {
                for (TableRef tableRef : lookupTableRefs) {
                    result = tableRef.getColumn(column);
                    if (result != null)
                        break;
                }
            }
        }

        if (result == null)
            throw new IllegalArgumentException("Column not found by " + column);

        return result;
    }

    // find by unique name, that must uniquely identifies a table in the model
    public TableRef findTable(String table) {
        TableRef result = tableNameMap.get(table);
        if (result == null) {
            throw new IllegalArgumentException("Table not found by " + table);
        }
        return result;
    }
    
    // find by table identity, that may match multiple tables in the model
    public TableRef findFirstTable(String tableIdentity) {
        if (factTableRef.getTableIdentity().equals(tableIdentity))
            return factTableRef;
        
        for (TableRef lookup : lookupTableRefs) {
            if (lookup.getTableIdentity().equals(tableIdentity))
                return lookup;
        }
        throw new IllegalArgumentException("Table not found by " + tableIdentity);
    }

    public void init(KylinConfig config, Map<String, TableDesc> tables) {
        this.config = config;
        
        lookupTableRefs.clear();
        aliasMap.clear();
        tableNameMap.clear();
        
        initTableAlias(tables);
        initJoinColumns();
        ModelDimensionDesc.capicalizeStrings(dimensions);
        initPartitionDesc();
    }

    private void initTableAlias(Map<String, TableDesc> tables) {
        factTable = factTable.toUpperCase();
        
        if (tables.containsKey(factTable) == false)
            throw new IllegalStateException("Fact table does not exist:" + factTable);
        
        TableDesc factDesc = tables.get(factTable);
        factTableRef = new TableRef(this, factDesc.getName(), factDesc);
        addAlias(factTableRef);
        
        for (LookupDesc lookup : lookups) {
            lookup.setTable(lookup.getTable().toUpperCase());
            
            if (tables.containsKey(lookup.getTable()) == false)
                throw new IllegalStateException("Lookup table does not exist:" + lookup.getTable());
            
            TableDesc tableDesc = tables.get(lookup.getTable());
            String alias = lookup.getAlias();
            if (alias == null)
                alias = tableDesc.getName();
            TableRef ref = new TableRef(this, alias, tableDesc);
            lookup.setTableRef(ref);
            lookupTableRefs.add(ref);
            addAlias(ref);
        }

        tableNameMap.putAll(aliasMap);
    }

    private void addAlias(TableRef ref) {
        String alias = ref.getAlias();
        if (aliasMap.containsKey(alias))
            throw new IllegalStateException("Alias '" + alias + "' ref to multiple tables: " + ref.getTableIdentity() + ", " + aliasMap.get(alias).getTableIdentity());
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

    private void initPartitionDesc() {
        if (this.partitionDesc != null)
            this.partitionDesc.init(this);
    }

    private void initJoinColumns() {
        for (LookupDesc lookup : this.lookups) {
            TableRef dimTable = lookup.getTableRef();
            JoinDesc join = lookup.getJoin();
            if (join == null)
                continue;

            StringUtil.toUpperCaseArray(join.getForeignKey(), join.getForeignKey());
            StringUtil.toUpperCaseArray(join.getPrimaryKey(), join.getPrimaryKey());

            // primary key
            String[] pks = join.getPrimaryKey();
            TblColRef[] pkCols = new TblColRef[pks.length];
            for (int i = 0; i < pks.length; i++) {
                TblColRef col = dimTable.getColumn(pks[i]);
                if (col == null) {
                    throw new IllegalStateException("Can't find column " + pks[i] + " in table " + dimTable.getTableIdentity());
                }
                pkCols[i] = col;
            }
            join.setPrimaryKeyColumns(pkCols);

            // foreign key
            String[] fks = join.getForeignKey();
            TblColRef[] fkCols = new TblColRef[fks.length];
            for (int i = 0; i < fks.length; i++) {
                TblColRef col = factTableRef.getColumn(fks[i]);
                if (col == null) {
                    throw new IllegalStateException("Can't find column " + fks[i] + " in table " + this.getFactTable());
                }
                fkCols[i] = col;
            }
            join.setForeignKeyColumns(fkCols);

            // Validate join in dimension
            if (pkCols.length != fkCols.length) {
                throw new IllegalStateException("Primary keys(" + lookup.getTable() + ")" + Arrays.toString(pks) + " are not consistent with Foreign keys(" + this.getFactTable() + ") " + Arrays.toString(fks));
            }
            for (int i = 0; i < fkCols.length; i++) {
                if (!fkCols[i].getDatatype().equals(pkCols[i].getDatatype())) {
                    logger.warn("PK " + lookup.getTable() + "." + pkCols[i].getName() + "." + pkCols[i].getDatatype() + " are not consistent with FK " + this.getFactTable() + "." + fkCols[i].getName() + "." + fkCols[i].getDatatype());
                }
            }

        }
    }

    /** * Add error info and thrown exception out
     *
     * @param message
     */
    public void addError(String message) {
        addError(message, false);
    }

    /**
     * @param message
     *            error message
     * @param silent
     *            if throw exception
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
        if (!getFactTable().equals(modelDesc.getFactTable()))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + name.hashCode();
        result = 31 * result + getFactTable().hashCode();
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

    public String[] getMetrics() {
        return metrics;
    }

    public void setDimensions(List<ModelDimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public void setMetrics(String[] metrics) {
        this.metrics = metrics;
    }

    public static DataModelDesc getCopyOf(DataModelDesc dataModelDesc) {
        DataModelDesc newDataModelDesc = new DataModelDesc();
        newDataModelDesc.setName(dataModelDesc.getName());
        newDataModelDesc.setDescription(dataModelDesc.getDescription());
        newDataModelDesc.setDimensions(dataModelDesc.getDimensions());
        newDataModelDesc.setFilterCondition(dataModelDesc.getFilterCondition());
        newDataModelDesc.setFactTable(dataModelDesc.getFactTable());
        newDataModelDesc.setLookups(dataModelDesc.getLookups());
        newDataModelDesc.setMetrics(dataModelDesc.getMetrics());
        newDataModelDesc.setPartitionDesc(PartitionDesc.getCopyOf(dataModelDesc.getPartitionDesc()));
        newDataModelDesc.updateRandomUuid();
        return newDataModelDesc;
    }
}

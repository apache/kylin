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
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.metadata.MetadataConstants;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class DataModelDesc extends RootPersistentEntity {

    public static enum RealizationCapacity {
        SMALL, MEDIUM, LARGE
    }

    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("fact_table")
    private String factTable;

    @JsonProperty("lookups")
    private LookupDesc[] lookups;

    @JsonProperty("dimensions")
    private List<DimensionDesc> dimensions;

    @JsonProperty("metrics")
    private String[] metrics;

    @JsonProperty("filter_condition")
    private String filterCondition;
    @JsonProperty("partition_desc")
    PartitionDesc partitionDesc;

    @JsonProperty("capacity")
    private RealizationCapacity capacity = RealizationCapacity.MEDIUM;

    private TableDesc factTableDesc;

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = new ArrayList<String>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public TableDesc getFactTableDesc() {
        return factTableDesc;
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

    public void init(Map<String, TableDesc> tables) {
        this.factTable = this.factTable.toUpperCase();
        this.factTableDesc = tables.get(this.factTable.toUpperCase());
        if (factTableDesc == null) {
            throw new IllegalStateException("Fact table does not exist:" + this.factTable);
        }

        initJoinColumns(tables);
        DimensionDesc.capicalizeStrings(dimensions);
        initPartitionDesc(tables);
    }

    private void initPartitionDesc(Map<String, TableDesc> tables) {
        if (this.partitionDesc != null)
            this.partitionDesc.init(tables);
    }

    private void initJoinColumns(Map<String, TableDesc> tables) {
        // join columns may or may not present in cube;
        // here we don't modify 'allColumns' and 'dimensionColumns';
        // initDimensionColumns() will do the update
        for (LookupDesc lookup : this.lookups) {
            lookup.setTable(lookup.getTable().toUpperCase());
            TableDesc dimTable = tables.get(lookup.getTable());
            if (dimTable == null) {
                throw new IllegalStateException("Table " + lookup.getTable() + " does not exist for " + this);
            }

            JoinDesc join = lookup.getJoin();
            if (join == null)
                continue;

            StringUtil.toUpperCaseArray(join.getForeignKey(), join.getForeignKey());
            StringUtil.toUpperCaseArray(join.getPrimaryKey(), join.getPrimaryKey());

            // primary key
            String[] pks = join.getPrimaryKey();
            TblColRef[] pkCols = new TblColRef[pks.length];
            for (int i = 0; i < pks.length; i++) {
                ColumnDesc col = dimTable.findColumnByName(pks[i]);
                if (col == null) {
                    throw new IllegalStateException("Can't find column " + pks[i] + " in table " + dimTable.getIdentity());
                }
                TblColRef colRef = new TblColRef(col);
                pks[i] = colRef.getName();
                pkCols[i] = colRef;
            }
            join.setPrimaryKeyColumns(pkCols);

            // foreign key
            String[] fks = join.getForeignKey();
            TblColRef[] fkCols = new TblColRef[fks.length];
            for (int i = 0; i < fks.length; i++) {
                ColumnDesc col = factTableDesc.findColumnByName(fks[i]);
                if (col == null) {
                    throw new IllegalStateException("Can't find column " + fks[i] + " in table " + this.getFactTable());
                }
                TblColRef colRef = new TblColRef(col);
                fks[i] = colRef.getName();
                fkCols[i] = colRef;
            }
            join.setForeignKeyColumns(fkCols);

            // Validate join in dimension
            if (pkCols.length != fkCols.length) {
                throw new IllegalStateException("Primary keys(" + lookup.getTable() + ")" + Arrays.toString(pks) + " are not consistent with Foreign keys(" + this.getFactTable() + ") " + Arrays.toString(fks));
            }
            for (int i = 0; i < fkCols.length; i++) {
                if (!fkCols[i].getDatatype().equals(pkCols[i].getDatatype())) {
                    throw new IllegalStateException("Primary key " + lookup.getTable() + "." + pkCols[i].getName() + "." + pkCols[i].getDatatype() + " are not consistent with Foreign key " + this.getFactTable() + "." + fkCols[i].getName() + "." + fkCols[i].getDatatype());
                }
            }

        }
    }

    /**
     * Add error info and thrown exception out
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

    public List<DimensionDesc> getDimensions() {
        return dimensions;
    }

    public String[] getMetrics() {
        return metrics;
    }

    public void setDimensions(List<DimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public void setMetrics(String[] metrics) {
        this.metrics = metrics;
    }

    public static DataModelDesc getCopyOf(DataModelDesc dataModelDesc) {
        DataModelDesc newDataModelDesc = new DataModelDesc();
        newDataModelDesc.setName(dataModelDesc.getName());
        newDataModelDesc.setCapacity(dataModelDesc.getCapacity());
        newDataModelDesc.setDescription(dataModelDesc.getDescription());
        newDataModelDesc.setDimensions(dataModelDesc.getDimensions());
        newDataModelDesc.setFilterCondition(dataModelDesc.getFilterCondition());
        newDataModelDesc.setFactTable(dataModelDesc.getFactTable());
        newDataModelDesc.setLookups(dataModelDesc.getLookups());
        newDataModelDesc.setMetrics(dataModelDesc.getMetrics());
        newDataModelDesc.setPartitionDesc(PartitionDesc.getCopyOf(dataModelDesc.getPartitionDesc()));
        newDataModelDesc.updateVersionAndRandomUuid();
        return newDataModelDesc;
    }
}

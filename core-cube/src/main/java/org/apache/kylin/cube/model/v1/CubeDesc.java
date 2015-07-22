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

package org.apache.kylin.cube.model.v1;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.cube.model.HierarchyDesc;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeDesc extends RootPersistentEntity {

    public static enum CubeCapacity {
        SMALL, MEDIUM, LARGE;
    }

    public static enum DeriveType {
        LOOKUP, PK_FK
    }

    public static class DeriveInfo {
        public DeriveType type;
        public DimensionDesc dimension;
        public TblColRef[] columns;
        public boolean isOneToOne; // only used when ref from derived to host

        DeriveInfo(DeriveType type, DimensionDesc dimension, TblColRef[] columns, boolean isOneToOne) {
            this.type = type;
            this.dimension = dimension;
            this.columns = columns;
            this.isOneToOne = isOneToOne;
        }

        @Override
        public String toString() {
            return "DeriveInfo [type=" + type + ", dimension=" + dimension + ", columns=" + Arrays.toString(columns) + ", isOneToOne=" + isOneToOne + "]";
        }

    }

    private KylinConfig config;

    @JsonProperty("name")
    private String name;
    @JsonProperty("description")
    private String description;
    @JsonProperty("fact_table")
    private String factTable;
    @JsonProperty("null_string")
    private String[] nullStrings;
    @JsonProperty("filter_condition")
    private String filterCondition;
    @JsonProperty("cube_partition_desc")
    CubePartitionDesc cubePartitionDesc;
    @JsonProperty("dimensions")
    private List<DimensionDesc> dimensions;
    @JsonProperty("measures")
    private List<MeasureDesc> measures;
    @JsonProperty("rowkey")
    private RowKeyDesc rowkey;
    @JsonProperty("hbase_mapping")
    private HBaseMappingDesc hbaseMapping;
    @JsonProperty("signature")
    private String signature;
    @JsonProperty("capacity")
    private CubeCapacity capacity = CubeCapacity.MEDIUM;
    @JsonProperty("notify_list")
    private List<String> notifyList;

    private Map<String, Map<String, TblColRef>> columnMap = new HashMap<String, Map<String, TblColRef>>();
    private LinkedHashSet<TblColRef> allColumns = new LinkedHashSet<TblColRef>();
    private LinkedHashSet<TblColRef> dimensionColumns = new LinkedHashSet<TblColRef>();
    private Map<TblColRef, DeriveInfo> derivedToHostMap = Maps.newHashMap();
    private Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedMap = Maps.newHashMap();

    /**
     * Error messages during resolving json metadata
     */
    private List<String> errors = new ArrayList<String>();

    /**
     * @return all columns this cube can support, including derived
     */
    public Set<TblColRef> listAllColumns() {
        return allColumns;
    }

    /**
     * @return dimension columns including derived, BUT NOT measures
     */
    public Set<TblColRef> listDimensionColumnsIncludingDerived() {
        return dimensionColumns;
    }

    /**
     * @return dimension columns excluding derived and measures
     */
    public List<TblColRef> listDimensionColumnsExcludingDerived() {
        List<TblColRef> result = new ArrayList<TblColRef>();
        for (TblColRef col : dimensionColumns) {
            if (isDerived(col) == false)
                result.add(col);
        }
        return result;
    }

    /**
     * Find FunctionDesc by Full Expression.
     * 
     * @return
     */
    public FunctionDesc findFunctionOnCube(FunctionDesc manualFunc) {
        for (MeasureDesc m : measures) {
            if (m.getFunction().equals(manualFunc))
                return m.getFunction();
        }
        return null;
    }

    public TblColRef findColumnRef(String table, String column) {
        Map<String, TblColRef> cols = columnMap.get(table);
        if (cols == null)
            return null;
        else
            return cols.get(column);
    }

    public DimensionDesc findDimensionByColumn(TblColRef col) {
        for (DimensionDesc dim : dimensions) {
            if (ArrayUtils.contains(dim.getColumnRefs(), col))
                return dim;
        }
        return null;
    }

    public DimensionDesc findDimensionByTable(String lookupTableName) {
        lookupTableName = lookupTableName.toUpperCase();
        for (DimensionDesc dim : dimensions)
            if (dim.getTable() != null && dim.getTable().equals(lookupTableName))
                return dim;
        return null;
    }

    public DimensionDesc findDimensionByName(String dimName) {
        dimName = dimName.toUpperCase();
        for (DimensionDesc dim : dimensions) {
            if (dimName.equals(dim.getName()))
                return dim;
        }
        return null;
    }

    public TblColRef findPKByFK(TblColRef fk) {
        assert isFactTable(fk.getTable());

        TblColRef candidate = null;

        for (DimensionDesc dim : dimensions) {
            JoinDesc join = dim.getJoin();
            if (join == null)
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

    /**
     * Get all functions from each measure.
     * 
     * @return
     */
    public List<FunctionDesc> listAllFunctions() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();
        for (MeasureDesc m : measures) {
            functions.add(m.getFunction());
        }
        return functions;
    }

    public List<TableDesc> listTables() {
        MetadataManager metaMgr = MetadataManager.getInstance(config);
        HashSet<String> tableNames = new HashSet<String>();
        List<TableDesc> result = new ArrayList<TableDesc>();

        tableNames.add(factTable.toUpperCase());
        for (DimensionDesc dim : dimensions) {
            String table = dim.getTable();
            if (table != null)
                tableNames.add(table.toUpperCase());
        }

        for (String tableName : tableNames) {
            result.add(metaMgr.getTableDesc(tableName));
        }

        return result;
    }

    public boolean isFactTable(String factTable) {
        return this.factTable.equalsIgnoreCase(factTable);
    }

    public boolean isDerived(TblColRef col) {
        return derivedToHostMap.containsKey(col);
    }

    public DeriveInfo getHostInfo(TblColRef derived) {
        return derivedToHostMap.get(derived);
    }

    public Map<Array<TblColRef>, List<DeriveInfo>> getHostToDerivedInfo(List<TblColRef> rowCols, Collection<TblColRef> wantedCols) {
        Map<Array<TblColRef>, List<DeriveInfo>> result = new HashMap<Array<TblColRef>, List<DeriveInfo>>();
        for (Entry<Array<TblColRef>, List<DeriveInfo>> entry : hostToDerivedMap.entrySet()) {
            Array<TblColRef> hostCols = entry.getKey();
            boolean hostOnRow = rowCols.containsAll(Arrays.asList(hostCols.data));
            if (!hostOnRow)
                continue;

            List<DeriveInfo> wantedInfo = new ArrayList<DeriveInfo>();
            for (DeriveInfo info : entry.getValue()) {
                if (wantedCols == null || Collections.disjoint(wantedCols, Arrays.asList(info.columns)) == false) // has
                                                                                                                  // any
                                                                                                                  // wanted
                                                                                                                  // columns?
                    wantedInfo.add(info);
            }

            if (wantedInfo.size() > 0)
                result.put(hostCols, wantedInfo);
        }
        return result;
    }

    public String getResourcePath() {
        return getCubeDescResourcePath(name);
    }

    public static String getCubeDescResourcePath(String descName) {
        return ResourceStore.CUBE_DESC_RESOURCE_ROOT + "/" + descName + MetadataConstants.FILE_SURFIX;
    }

    // ============================================================================

    public HBaseMappingDesc getHBaseMapping() {
        return hbaseMapping;
    }

    public void setHBaseMapping(HBaseMappingDesc hbaseMapping) {
        this.hbaseMapping = hbaseMapping;
    }

    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

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

    public String getFactTable() {
        return factTable;
    }

    public void setFactTable(String factTable) {
        this.factTable = factTable;
    }

    public String[] getNullStrings() {
        return nullStrings;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    public CubePartitionDesc getCubePartitionDesc() {
        return cubePartitionDesc;
    }

    public void setCubePartitionDesc(CubePartitionDesc cubePartitionDesc) {
        this.cubePartitionDesc = cubePartitionDesc;
    }

    public List<DimensionDesc> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<DimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public List<MeasureDesc> getMeasures() {
        return measures;
    }

    public void setMeasures(List<MeasureDesc> measures) {
        this.measures = measures;
    }

    public RowKeyDesc getRowkey() {
        return rowkey;
    }

    public void setRowkey(RowKeyDesc rowkey) {
        this.rowkey = rowkey;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }

    public CubeCapacity getCapacity() {
        return capacity;
    }

    public void setCapacity(CubeCapacity capacity) {
        this.capacity = capacity;
    }

    public List<String> getNotifyList() {
        return notifyList;
    }

    public void setNotifyList(List<String> notifyList) {
        this.notifyList = notifyList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CubeDesc cubeDesc = (CubeDesc) o;

        if (!name.equals(cubeDesc.name))
            return false;
        if (!factTable.equals(cubeDesc.factTable))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + name.hashCode();
        result = 31 * result + factTable.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CubeDesc [name=" + name + ", factTable=" + factTable + ", cubePartitionDesc=" + cubePartitionDesc + ", dimensions=" + dimensions + ", measures=" + measures + ", rowkey=" + rowkey + ", hbaseMapping=" + hbaseMapping + "]";
    }

    public String calculateSignature() {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            StringBuilder sigString = new StringBuilder();
            sigString.append(this.name).append("|").append(this.factTable).append("|").append(JsonUtil.writeValueAsString(this.cubePartitionDesc)).append("|").append(JsonUtil.writeValueAsString(this.dimensions)).append("|").append(JsonUtil.writeValueAsString(this.measures)).append("|").append(JsonUtil.writeValueAsString(this.rowkey)).append("|").append(JsonUtil.writeValueAsString(this.hbaseMapping));

            byte[] signature = md.digest(sigString.toString().getBytes());
            return new String(Base64.encodeBase64(signature));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to calculate signature");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to calculate signature");
        }
    }

    public Map<String, TblColRef> buildColumnNameAbbreviation() {
        Map<String, TblColRef> r = new HashMap<String, TblColRef>();
        for (TblColRef col : listDimensionColumnsExcludingDerived()) {
            r.put(col.getName(), col);
        }
        return r;
    }

    public void init(KylinConfig config, Map<String, TableDesc> tables) {
        this.errors.clear();
        this.config = config;

        if (factTable != null)
            factTable = factTable.toUpperCase();

        for (DimensionDesc dim : dimensions) {
            dim.init(tables);
        }

        sortDimAndMeasure();

        initJoinColumns(tables);
        initDimensionColumns(tables);
        initMeasureColumns(tables);

        /** TODO: should do the initialization after conver
        rowkey.init(this);
        if (hbaseMapping != null) {
            hbaseMapping.init(this);
        }
        */
        initMeasureReferenceToColumnFamily();

        if (null != this.cubePartitionDesc) {
            this.cubePartitionDesc.init(columnMap);
        }

        // check all dimension columns are presented on rowkey
        List<TblColRef> dimCols = listDimensionColumnsExcludingDerived();
        if (rowkey.getRowKeyColumns().length != dimCols.size()) {
            addError("RowKey columns count (" + rowkey.getRowKeyColumns().length + ") does not match dimension columns count (" + dimCols.size() + "). ");
        }
    }

    private void initDimensionColumns(Map<String, TableDesc> tables) {
        // fill back ColRefDesc
        for (DimensionDesc dim : dimensions) {
            TableDesc dimTable = tables.get(dim.getTable());
            JoinDesc join = dim.getJoin();

            ArrayList<TblColRef> dimColList = new ArrayList<TblColRef>();
            ArrayList<TblColRef> hostColList = new ArrayList<TblColRef>();

            // dimension column
            if (dim.getColumn() != null) {
                if ("{FK}".equals(dim.getColumn())) {
                    for (TblColRef ref : join.getForeignKeyColumns()) {
                        TblColRef inited = initDimensionColRef(ref);
                        dimColList.add(inited);
                        hostColList.add(inited);
                    }
                } else {
                    TblColRef ref = initDimensionColRef(dimTable, dim.getColumn());
                    dimColList.add(ref);
                    hostColList.add(ref);
                }
            }
            // hierarchy columns
            if (dim.getHierarchy() != null) {
                for (HierarchyDesc hier : dim.getHierarchy()) {
                    TblColRef ref = initDimensionColRef(dimTable, hier.getColumn());
                    hier.setColumnRef(ref);
                    dimColList.add(ref);
                }
                if (hostColList.isEmpty()) { // the last hierarchy could serve
                                             // as host when col is
                                             // unspecified
                    hostColList.add(dimColList.get(dimColList.size() - 1));
                }
            }
            TblColRef[] dimCols = (TblColRef[]) dimColList.toArray(new TblColRef[dimColList.size()]);
            dim.setColumnRefs(dimCols);

            // lookup derived columns
            TblColRef[] hostCols = (TblColRef[]) hostColList.toArray(new TblColRef[hostColList.size()]);
            String[] derived = dim.getDerived();
            if (derived != null) {
                String[][] split = splitDerivedColumnAndExtra(derived);
                String[] derivedNames = split[0];
                String[] derivedExtra = split[1];
                TblColRef[] derivedCols = new TblColRef[derivedNames.length];
                for (int i = 0; i < derivedNames.length; i++) {
                    derivedCols[i] = initDimensionColRef(dimTable, derivedNames[i]);
                }
                initDerivedMap(hostCols, DeriveType.LOOKUP, dim, derivedCols, derivedExtra);
            }

            // FK derived column
            if (join != null) {
                TblColRef[] fk = join.getForeignKeyColumns();
                TblColRef[] pk = join.getPrimaryKeyColumns();
                for (int i = 0; i < fk.length; i++) {
                    int find = ArrayUtils.indexOf(hostCols, fk[i]);
                    if (find >= 0) {
                        TblColRef derivedCol = initDimensionColRef(pk[i]);
                        initDerivedMap(hostCols[find], DeriveType.PK_FK, dim, derivedCol);
                    }
                }
                for (int i = 0; i < pk.length; i++) {
                    int find = ArrayUtils.indexOf(hostCols, pk[i]);
                    if (find >= 0) {
                        TblColRef derivedCol = initDimensionColRef(fk[i]);
                        initDerivedMap(hostCols[find], DeriveType.PK_FK, dim, derivedCol);
                    }
                }
            }
        }
    }

    private String[][] splitDerivedColumnAndExtra(String[] derived) {
        String[] cols = new String[derived.length];
        String[] extra = new String[derived.length];
        for (int i = 0; i < derived.length; i++) {
            String str = derived[i];
            int cut = str.indexOf(":");
            if (cut >= 0) {
                cols[i] = str.substring(0, cut);
                extra[i] = str.substring(cut + 1).trim();
            } else {
                cols[i] = str;
                extra[i] = "";
            }
        }
        return new String[][] { cols, extra };
    }

    private void initDerivedMap(TblColRef hostCol, DeriveType type, DimensionDesc dimension, TblColRef derivedCol) {
        initDerivedMap(new TblColRef[] { hostCol }, type, dimension, new TblColRef[] { derivedCol }, null);
    }

    private void initDerivedMap(TblColRef[] hostCols, DeriveType type, DimensionDesc dimension, TblColRef[] derivedCols, String[] extra) {
        if (hostCols.length == 0 || derivedCols.length == 0)
            throw new IllegalStateException("host/derived columns must not be empty");

        Array<TblColRef> hostColArray = new Array<TblColRef>(hostCols);
        List<DeriveInfo> infoList = hostToDerivedMap.get(hostColArray);
        if (infoList == null) {
            hostToDerivedMap.put(hostColArray, infoList = new ArrayList<DeriveInfo>());
        }
        infoList.add(new DeriveInfo(type, dimension, derivedCols, false));

        for (int i = 0; i < derivedCols.length; i++) {
            TblColRef derivedCol = derivedCols[i];
            boolean isOneToOne = type == DeriveType.PK_FK || ArrayUtils.contains(hostCols, derivedCol) || (extra != null && extra[i].contains("1-1"));
            derivedToHostMap.put(derivedCol, new DeriveInfo(type, dimension, hostCols, isOneToOne));
        }
    }

    private TblColRef initDimensionColRef(TableDesc table, String colName) {
        ColumnDesc col = table.findColumnByName(colName);
        if (col == null)
            throw new IllegalArgumentException("No column '" + colName + "' found in table " + table);

        TblColRef ref = new TblColRef(col);
        return initDimensionColRef(ref);
    }

    private TblColRef initDimensionColRef(TblColRef ref) {
        TblColRef existing = findColumnRef(ref.getTable(), ref.getName());
        if (existing != null) {
            return existing;
        }

        allColumns.add(ref);
        dimensionColumns.add(ref);

        Map<String, TblColRef> cols = columnMap.get(ref.getTable());
        if (cols == null) {
            columnMap.put(ref.getTable(), cols = new HashMap<String, TblColRef>());
        }
        cols.put(ref.getName(), ref);
        return ref;
    }

    private void initJoinColumns(Map<String, TableDesc> tables) {
        // join columns may or may not present in cube;
        // here we don't modify 'allColumns' and 'dimensionColumns';
        // initDimensionColumns() will do the update
        for (DimensionDesc dim : dimensions) {
            TableDesc dimTable = tables.get(dim.getTable());

            JoinDesc join = dim.getJoin();
            if (join == null)
                continue;

            // primary key
            String[] pks = join.getPrimaryKey();
            TblColRef[] pkCols = new TblColRef[pks.length];
            for (int i = 0; i < pks.length; i++) {
                ColumnDesc col = dimTable.findColumnByName(pks[i]);
                if (col == null) {
                    addError("Can't find column " + pks[i] + " in table " + dimTable.getName());
                }
                TblColRef colRef = new TblColRef(col);
                pks[i] = colRef.getName();
                pkCols[i] = colRef;
            }
            join.setPrimaryKeyColumns(pkCols);
            // foreign key
            TableDesc factTable = tables.get(this.factTable);
            if (factTable == null) {
                addError("Fact table does not exist:" + this.factTable);
            }
            String[] fks = join.getForeignKey();
            TblColRef[] fkCols = new TblColRef[fks.length];
            for (int i = 0; i < fks.length; i++) {
                ColumnDesc col = factTable.findColumnByName(fks[i]);
                if (col == null) {
                    addError("Can't find column " + fks[i] + " in table " + this.factTable);
                }
                TblColRef colRef = new TblColRef(col);
                fks[i] = colRef.getName();
                fkCols[i] = colRef;
            }
            join.setForeignKeyColumns(fkCols);
            // Validate join in dimension
            if (pkCols.length != fkCols.length) {
                addError("Primary keys(" + dim.getTable() + ")" + Arrays.toString(pks) + " are not consistent with Foreign keys(" + this.factTable + ") " + Arrays.toString(fks));
            }
            for (int i = 0; i < fkCols.length; i++) {
                if (!fkCols[i].getDatatype().equals(pkCols[i].getDatatype())) {
                    addError("Primary key " + dim.getTable() + "." + pkCols[i].getName() + "." + pkCols[i].getDatatype() + " are not consistent with Foreign key " + this.factTable + "." + fkCols[i].getName() + "." + fkCols[i].getDatatype());
                }
            }

        }
    }

    private void initMeasureColumns(Map<String, TableDesc> tables) {
        if (measures == null || measures.isEmpty()) {
            return;
        }

        TableDesc factTable = tables.get(getFactTable());
        for (MeasureDesc m : measures) {
            m.setName(m.getName().toUpperCase());

            if (m.getDependentMeasureRef() != null) {
                m.setDependentMeasureRef(m.getDependentMeasureRef().toUpperCase());
            }

            FunctionDesc f = m.getFunction();
            f.setExpression(f.getExpression().toUpperCase());
            f.initReturnDataType();

            ParameterDesc p = f.getParameter();
            p.normalizeColumnValue();

            if (p.isColumnType()) {
                ArrayList<TblColRef> colRefs = Lists.newArrayList();
                for (String cName : p.getValue().split("\\s*,\\s*")) {
                    ColumnDesc sourceColumn = factTable.findColumnByName(cName);
                    TblColRef colRef = new TblColRef(sourceColumn);
                    colRefs.add(colRef);
                    allColumns.add(colRef);
                }
                if (colRefs.isEmpty() == false)
                    p.setColRefs(colRefs);
            }
        }
    }

    private void initMeasureReferenceToColumnFamily() {
        if (measures == null || measures.size() == 0)
            return;

        Map<String, MeasureDesc> measureCache = new HashMap<String, MeasureDesc>();
        for (MeasureDesc m : measures)
            measureCache.put(m.getName(), m);

        for (HBaseColumnFamilyDesc cf : getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc c : cf.getColumns()) {
                MeasureDesc[] measureDescs = new MeasureDesc[c.getMeasureRefs().length];
                for (int i = 0; i < c.getMeasureRefs().length; i++) {
                    measureDescs[i] = measureCache.get(c.getMeasureRefs()[i]);
                }
                c.setMeasures(measureDescs);
                c.setColumnFamilyName(cf.getName());
            }
        }
    }

    private void sortDimAndMeasure() {
        sortDimensionsByID();
        sortMeasuresByID();
        for (DimensionDesc dim : dimensions) {
            sortHierarchiesByLevel(dim.getHierarchy());
        }
    }

    private void sortDimensionsByID() {
        Collections.sort(dimensions, new Comparator<DimensionDesc>() {
            @Override
            public int compare(DimensionDesc d1, DimensionDesc d2) {
                Integer id1 = d1.getId();
                Integer id2 = d2.getId();
                return id1.compareTo(id2);
            }
        });
    }

    private void sortMeasuresByID() {
        if (measures == null) {
            measures = Lists.newArrayList();
        }

        Collections.sort(measures, new Comparator<MeasureDesc>() {
            @Override
            public int compare(MeasureDesc m1, MeasureDesc m2) {
                Integer id1 = m1.getId();
                Integer id2 = m2.getId();
                return id1.compareTo(id2);
            }
        });
    }

    private void sortHierarchiesByLevel(HierarchyDesc[] hierarchies) {
        if (hierarchies != null) {
            Arrays.sort(hierarchies, new Comparator<HierarchyDesc>() {
                @Override
                public int compare(HierarchyDesc h1, HierarchyDesc h2) {
                    Integer level1 = Integer.parseInt(h1.getLevel());
                    Integer level2 = Integer.parseInt(h2.getLevel());
                    return level1.compareTo(level2);
                }
            });
        }
    }

    public boolean hasHolisticCountDistinctMeasures() {
        for (MeasureDesc measure : measures) {
            if (measure.getFunction().isHolisticCountDistinct()) {
                return true;
            }
        }
        return false;
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

}

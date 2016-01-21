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

package org.apache.kylin.cube.model;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
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
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeDesc extends RootPersistentEntity {

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
    private DataModelDesc model;

    @JsonProperty("name")
    private String name;
    @JsonProperty("model_name")
    private String modelName;
    @JsonProperty("description")
    private String description;
    @JsonProperty("null_string")
    private String[] nullStrings;
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
    @JsonProperty("notify_list")
    private List<String> notifyList;
    @JsonProperty("status_need_notify")
    private List<String> statusNeedNotify = Collections.emptyList();

    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;
    @JsonProperty("partition_date_end")
    private long partitionDateEnd = 3153600000000l;
    @JsonProperty("auto_merge_time_ranges")
    private long[] autoMergeTimeRanges;
    @JsonProperty("retention_range")
    private long retentionRange = 0;

    @JsonProperty("engine_type")
    private int engineType = IEngineAware.ID_MR_V1;
    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_HBASE;
    @JsonProperty("override_kylin_properties")
    private LinkedHashMap<String, String> overrideKylinProps = new LinkedHashMap<String, String>();

    private Map<String, Map<String, TblColRef>> columnMap = new HashMap<String, Map<String, TblColRef>>();
    private LinkedHashSet<TblColRef> allColumns = new LinkedHashSet<TblColRef>();
    private LinkedHashSet<TblColRef> dimensionColumns = new LinkedHashSet<TblColRef>();

    private Map<TblColRef, DeriveInfo> derivedToHostMap = Maps.newHashMap();
    private Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedMap = Maps.newHashMap();

    public boolean isEnableSharding() {
        //in the future may extend to other storage that is shard-able
        return storageType == IStorageAware.ID_SHARDED_HBASE;
    }

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
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String descName) {
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

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public DataModelDesc getModel() {
        return model;
    }

    public void setModel(DataModelDesc model) {
        this.model = model;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getFactTable() {
        return model.getFactTable();
    }

    public TableDesc getFactTableDesc() {
        return model.getFactTableDesc();
    }

    public String[] getNullStrings() {
        return nullStrings;
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

    public List<String> getNotifyList() {
        return notifyList;
    }

    public void setNotifyList(List<String> notifyList) {
        this.notifyList = notifyList;
    }

    public List<String> getStatusNeedNotify() {
        return statusNeedNotify;
    }

    public void setStatusNeedNotify(List<String> statusNeedNotify) {
        this.statusNeedNotify = statusNeedNotify;
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
        if (!getFactTable().equals(cubeDesc.getFactTable()))
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
        return "CubeDesc [name=" + name + "]";
    }

    public boolean checkSignature() {
        if (StringUtils.isBlank(getSignature())) {
            return true;
        }
        return calculateSignature().equals(getSignature());
    }

    public String calculateSignature() {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
            StringBuilder sigString = new StringBuilder();
            sigString.append(this.name).append("|").append(this.getFactTable()).append("|").append(JsonUtil.writeValueAsString(this.model.getPartitionDesc())).append("|").append(JsonUtil.writeValueAsString(this.dimensions)).append("|").append(JsonUtil.writeValueAsString(this.measures)).append("|").append(JsonUtil.writeValueAsString(this.rowkey)).append("|").append(JsonUtil.writeValueAsString(this.hbaseMapping));

            byte[] signature = md.digest(sigString.toString().getBytes());
            return new String(Base64.encodeBase64(signature));
        } catch (NoSuchAlgorithmException | JsonProcessingException e) {
            throw new RuntimeException("Failed to calculate signature");
        }
    }

    public Map<String, TblColRef> buildColumnNameAbbreviation() {
        Map<String, TblColRef> r = new CaseInsensitiveStringMap<TblColRef>();
        for (TblColRef col : listDimensionColumnsExcludingDerived()) {
            r.put(col.getName(), col);
        }
        return r;
    }

    public void init(KylinConfig config, Map<String, TableDesc> tables) {
        this.errors.clear();
        this.config = config;

        if (this.modelName == null || this.modelName.length() == 0) {
            this.addError("The cubeDesc '" + this.getName() + "' doesn't have data model specified.");
        }

        this.model = MetadataManager.getInstance(config).getDataModelDesc(this.modelName);

        if (this.model == null) {
            this.addError("No data model found with name '" + modelName + "'.");
        }

        for (DimensionDesc dim : dimensions) {
            dim.init(this, tables);
        }

        sortDimAndMeasure();
        initDimensionColumns();
        initMeasureColumns();

        rowkey.init(this);
        if (hbaseMapping != null) {
            hbaseMapping.init(this);
        }

        initMeasureReferenceToColumnFamily();

        // check all dimension columns are presented on rowkey
        List<TblColRef> dimCols = listDimensionColumnsExcludingDerived();
        if (rowkey.getRowKeyColumns().length != dimCols.size()) {
            addError("RowKey columns count (" + rowkey.getRowKeyColumns().length + ") does not match dimension columns count (" + dimCols.size() + "). ");
        }
    }

    private void initDimensionColumns() {
        for (DimensionDesc dim : dimensions) {
            JoinDesc join = dim.getJoin();

            // init dimension columns
            ArrayList<TblColRef> dimCols = Lists.newArrayList();
            String[] colStrs = dim.getColumn();

            // when column is omitted, special case
            if (colStrs == null && dim.isDerived() || ArrayUtils.contains(colStrs, "{FK}")) {
                for (TblColRef col : join.getForeignKeyColumns()) {
                    dimCols.add(initDimensionColRef(col));
                }
            }
            // normal case
            else {
                if (colStrs == null || colStrs.length == 0)
                    throw new IllegalStateException("Dimension column must not be blank " + dim);

                for (String colStr : colStrs) {
                    dimCols.add(initDimensionColRef(dim, colStr));
                }

                // fill back column ref in hierarchy
                if (dim.isHierarchy()) {
                    for (int i = 0; i < dimCols.size(); i++)
                        dim.getHierarchy()[i].setColumnRef(dimCols.get(i));
                }
            }

            TblColRef[] dimColArray = (TblColRef[]) dimCols.toArray(new TblColRef[dimCols.size()]);
            dim.setColumnRefs(dimColArray);

            // init derived columns
            TblColRef[] hostCols = dimColArray;
            if (dim.isDerived()) {
                String[] derived = dim.getDerived();
                String[][] split = splitDerivedColumnAndExtra(derived);
                String[] derivedNames = split[0];
                String[] derivedExtra = split[1];
                TblColRef[] derivedCols = new TblColRef[derivedNames.length];
                for (int i = 0; i < derivedNames.length; i++) {
                    derivedCols[i] = initDimensionColRef(dim, derivedNames[i]);
                }
                initDerivedMap(hostCols, DeriveType.LOOKUP, dim, derivedCols, derivedExtra);
            }

            // PK-FK derive the other side
            if (join != null) {
                TblColRef[] fk = join.getForeignKeyColumns();
                TblColRef[] pk = join.getPrimaryKeyColumns();

                allColumns.addAll(Arrays.asList(fk));
                allColumns.addAll(Arrays.asList(pk));
                for (int i = 0; i < fk.length; i++) {
                    int find = ArrayUtils.indexOf(hostCols, fk[i]);
                    if (find >= 0) {
                        TblColRef derivedCol = initDimensionColRef(pk[i]);
                        initDerivedMap(hostCols[find], DeriveType.PK_FK, dim, derivedCol);
                    }
                }
                /** disable this code as we don't need fk be derived from pk
                 for (int i = 0; i < pk.length; i++) {
                 int find = ArrayUtils.indexOf(hostCols, pk[i]);
                 if (find >= 0) {
                 TblColRef derivedCol = initDimensionColRef(fk[i]);
                 initDerivedMap(hostCols[find], DeriveType.PK_FK, dim, derivedCol);
                 }
                 }
                 */
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

        // Although FK derives PK automatically, user unaware of this can declare PK as derived dimension explicitly.
        // In that case, derivedCols[] will contain a FK which is transformed from the PK by initDimensionColRef().
        // Must drop FK from derivedCols[] before continue.
        for (int i = 0; i < derivedCols.length; i++) {
            if (ArrayUtils.contains(hostCols, derivedCols[i])) {
                derivedCols = (TblColRef[]) ArrayUtils.remove(derivedCols, i);
                extra = (String[]) ArrayUtils.remove(extra, i);
                i--;
            }
        }

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

    private TblColRef initDimensionColRef(DimensionDesc dim, String colName) {
        TableDesc table = dim.getTableDesc();
        ColumnDesc col = table.findColumnByName(colName);
        if (col == null)
            throw new IllegalArgumentException("No column '" + colName + "' found in table " + table);

        TblColRef ref = new TblColRef(col);

        // always use FK instead PK, FK could be shared by more than one lookup tables
        JoinDesc join = dim.getJoin();
        if (join != null) {
            int idx = ArrayUtils.indexOf(join.getPrimaryKeyColumns(), ref);
            if (idx >= 0) {
                ref = join.getForeignKeyColumns()[idx];
            }
        }
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

    private void initMeasureColumns() {
        if (measures == null || measures.isEmpty()) {
            return;
        }

        TableDesc factTable = getFactTableDesc();
        for (MeasureDesc m : measures) {
            m.setName(m.getName().toUpperCase());

            if (m.getDependentMeasureRef() != null) {
                m.setDependentMeasureRef(m.getDependentMeasureRef().toUpperCase());
            }

            FunctionDesc func = m.getFunction();
            func.init(factTable);
            allColumns.addAll(func.getParameter().getColRefs());

            // verify holistic count distinct as a dependent measure
            if (func.isHolisticCountDistinct() && StringUtils.isBlank(m.getDependentMeasureRef())) {
                throw new IllegalStateException(m + " is a holistic count distinct but it has no DependentMeasureRef defined!");
            }
        }
    }

    private void initMeasureReferenceToColumnFamily() {
        if (measures == null || measures.size() == 0)
            return;

        Map<String, MeasureDesc> measureLookup = new HashMap<String, MeasureDesc>();
        for (MeasureDesc m : measures)
            measureLookup.put(m.getName(), m);
        Map<String, Integer> measureIndexLookup = new HashMap<String, Integer>();
        for (int i = 0; i < measures.size(); i++)
            measureIndexLookup.put(measures.get(i).getName(), i);

        for (HBaseColumnFamilyDesc cf : getHBaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc c : cf.getColumns()) {
                String[] colMeasureRefs = c.getMeasureRefs();
                MeasureDesc[] measureDescs = new MeasureDesc[colMeasureRefs.length];
                int[] measureIndex = new int[colMeasureRefs.length];
                for (int i = 0; i < colMeasureRefs.length; i++) {
                    measureDescs[i] = measureLookup.get(colMeasureRefs[i]);
                    measureIndex[i] = measureIndexLookup.get(colMeasureRefs[i]);
                }
                c.setMeasures(measureDescs);
                c.setMeasureIndex(measureIndex);
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

    public boolean hasMemoryHungryCountDistinctMeasures() {
        for (MeasureDesc measure : measures) {
            if (measure.getFunction().isCountDistinct()) {
                return true;
            }
        }
        return false;
    }
    
    public long getRetentionRange() {
        return retentionRange;
    }

    public void setRetentionRange(long retentionRange) {
        this.retentionRange = retentionRange;
    }

    public long[] getAutoMergeTimeRanges() {
        return autoMergeTimeRanges;
    }

    public void setAutoMergeTimeRanges(long[] autoMergeTimeRanges) {
        this.autoMergeTimeRanges = autoMergeTimeRanges;
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

    public HBaseMappingDesc getHbaseMapping() {
        return hbaseMapping;
    }

    public void setHbaseMapping(HBaseMappingDesc hbaseMapping) {
        this.hbaseMapping = hbaseMapping;
    }

    public void setNullStrings(String[] nullStrings) {
        this.nullStrings = nullStrings;
    }

    public int getStorageType() {
        return storageType;
    }

    public void setStorageType(int storageType) {
        this.storageType = storageType;
    }

    public int getEngineType() {
        return engineType;
    }

    public void setEngineType(int engineType) {
        this.engineType = engineType;
    }

    public long getPartitionDateStart() {
        return partitionDateStart;
    }

    public void setPartitionDateStart(long partitionDateStart) {
        this.partitionDateStart = partitionDateStart;
    }

    public long getPartitionDateEnd() {
        return partitionDateEnd;
    }

    public void setPartitionDateEnd(long partitionDateEnd) {
        this.partitionDateEnd = partitionDateEnd;
    }
    
    public List<TblColRef> getAllColumnsNeedDictionary() {
        List<TblColRef> result = Lists.newArrayList();

        for (RowKeyColDesc rowKeyColDesc : rowkey.getRowKeyColumns()) {
            TblColRef colRef = rowKeyColDesc.getColRef();
            if (rowkey.isUseDictionary(colRef)) {
                result.add(colRef);
            }
        }

        for (MeasureDesc measure : measures) {
            result.addAll(measure.getColumnsNeedDictionary());
        }
        return result;
    }

    public static CubeDesc getCopyOf(CubeDesc cubeDesc) {
        CubeDesc newCubeDesc = new CubeDesc();
        newCubeDesc.setName(cubeDesc.getName());
        newCubeDesc.setModelName(cubeDesc.getModelName());
        newCubeDesc.setDescription(cubeDesc.getDescription());
        newCubeDesc.setNullStrings(cubeDesc.getNullStrings());
        newCubeDesc.setDimensions(cubeDesc.getDimensions());
        newCubeDesc.setMeasures(cubeDesc.getMeasures());
        newCubeDesc.setRowkey(cubeDesc.getRowkey());
        newCubeDesc.setHBaseMapping(cubeDesc.getHBaseMapping());
        newCubeDesc.setSignature(cubeDesc.getSignature());
        newCubeDesc.setNotifyList(cubeDesc.getNotifyList());
        newCubeDesc.setStatusNeedNotify(cubeDesc.getStatusNeedNotify());
        newCubeDesc.setAutoMergeTimeRanges(cubeDesc.getAutoMergeTimeRanges());
        newCubeDesc.setPartitionDateStart(cubeDesc.getPartitionDateStart());
        newCubeDesc.setPartitionDateEnd(cubeDesc.getPartitionDateEnd());
        newCubeDesc.setRetentionRange(cubeDesc.getRetentionRange());
        newCubeDesc.setEngineType(cubeDesc.getEngineType());
        newCubeDesc.setStorageType(cubeDesc.getStorageType());
        newCubeDesc.setConfig(cubeDesc.getConfig());
        newCubeDesc.updateRandomUuid();
        return newCubeDesc;
    }
}

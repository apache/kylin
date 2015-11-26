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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.measure.MeasureType;
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
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeDesc extends RootPersistentEntity {

    public enum DeriveType {
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
    @JsonProperty("aggregation_gropus")
    private List<AggregationGroup> aggregationGroups;
    @JsonProperty("signature")
    private String signature;
    @JsonProperty("notify_list")
    private List<String> notifyList;
    @JsonProperty("status_need_notify")
    private List<String> statusNeedNotify = Collections.emptyList();
    @JsonProperty("auto_merge_time_ranges")
    private long[] autoMergeTimeRanges;
    @JsonProperty("retention_range")
    private long retentionRange = 0;

    @JsonProperty("engine_type")
    private int engineType = IEngineAware.ID_MR_V1;
    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_HBASE;

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
     * @return all functions from each measure.
     */
    public List<FunctionDesc> listAllFunctions() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();
        for (MeasureDesc m : measures) {
            functions.add(m.getFunction());
        }
        return functions;
    }

    public TblColRef findColumnRef(String table, String column) {
        Map<String, TblColRef> cols = columnMap.get(table);
        if (cols == null)
            return null;
        else
            return cols.get(column);
    }

    public DimensionDesc findDimensionByTable(String lookupTableName) {
        lookupTableName = lookupTableName.toUpperCase();
        for (DimensionDesc dim : dimensions)
            if (dim.getTable() != null && dim.getTable().equals(lookupTableName))
                return dim;
        return null;
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

    public List<AggregationGroup> getAggregationGroups() {
        return aggregationGroups;
    }

    public void setAggregationGroups(List<AggregationGroup> aggregationGroups) {
        this.aggregationGroups = aggregationGroups;
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

    public int getBuildLevel() {
        return Collections.max(Collections2.transform(aggregationGroups, new Function<AggregationGroup, Integer>() {
            @Nullable
            @Override
            public Integer apply(AggregationGroup input) {
                return input.getBuildLevel();
            }
        }));
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

        String calculated = calculateSignature();
        String saved = getSignature();
        return calculated.equals(saved);
    }

    public String calculateSignature() {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            StringBuilder sigString = new StringBuilder();
            sigString.append(this.name).append("|")//
                    .append(JsonUtil.writeValueAsString(this.modelName)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.dimensions)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.measures)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.rowkey)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.aggregationGroups)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.hbaseMapping));

            byte[] signature = md.digest(sigString.toString().toLowerCase().getBytes());
            String ret = new String(Base64.encodeBase64(signature));
            return ret;
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

        initDimensionColumns();
        initMeasureColumns();

        rowkey.init(this);
        for (AggregationGroup agg : this.aggregationGroups) {
            agg.init(this, rowkey);
        }

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
            String colStrs = dim.getColumn();

            // when column is omitted, special case
            if ((colStrs == null && dim.isDerived()) || ("{FK}".equalsIgnoreCase(colStrs))) {
                for (TblColRef col : join.getForeignKeyColumns()) {
                    dimCols.add(initDimensionColRef(col));
                }
            }
            // normal case
            else {
                if (StringUtils.isEmpty(colStrs))
                    throw new IllegalStateException("Dimension column must not be blank " + dim);

                dimCols.add(initDimensionColRef(dim, colStrs));

                //                // fill back column ref in hierarchy
                //                if (dim.isHierarchy()) {
                //                    for (int i = 0; i < dimCols.size(); i++)
                //                        dim.getHierarchy()[i].setColumnRef(dimCols.get(i));
                //                }
            }

            TblColRef[] dimColArray = dimCols.toArray(new TblColRef[dimCols.size()]);
            dim.setColumnRefs(dimColArray);

            // init derived columns
            if (dim.isDerived()) {
                String[] derived = dim.getDerived();
                String[][] split = splitDerivedColumnAndExtra(derived);
                String[] derivedNames = split[0];
                String[] derivedExtra = split[1];
                TblColRef[] derivedCols = new TblColRef[derivedNames.length];
                for (int i = 0; i < derivedNames.length; i++) {
                    derivedCols[i] = initDimensionColRef(dim, derivedNames[i]);
                }
                initDerivedMap(dimColArray, DeriveType.LOOKUP, dim, derivedCols, derivedExtra);
            }

            // PK-FK derive the other side
            if (join != null) {
                TblColRef[] fk = join.getForeignKeyColumns();
                TblColRef[] pk = join.getPrimaryKeyColumns();

                allColumns.addAll(Arrays.asList(fk));
                allColumns.addAll(Arrays.asList(pk));
                for (int i = 0; i < fk.length; i++) {
                    int find = ArrayUtils.indexOf(dimColArray, fk[i]);
                    if (find >= 0) {
                        TblColRef derivedCol = initDimensionColRef(pk[i]);
                        initDerivedMap(dimColArray[find], DeriveType.PK_FK, dim, derivedCol);
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

        for (HBaseColumnFamilyDesc cf : getHbaseMapping().getColumnFamily()) {
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

    public boolean hasHolisticCountDistinctMeasures() {
        for (MeasureDesc measure : measures) {
            if (measure.getFunction().isHolisticCountDistinct()) {
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

    public List<TblColRef> getAllColumnsNeedDictionary() {
        List<TblColRef> result = Lists.newArrayList();

        for (RowKeyColDesc rowKeyColDesc : rowkey.getRowKeyColumns()) {
            TblColRef colRef = rowKeyColDesc.getColRef();
            if (rowkey.isUseDictionary(colRef)) {
                result.add(colRef);
            }
        }

        for (MeasureDesc measure : measures) {
            MeasureType aggrType = MeasureType.create(measure.getFunction());
            result.addAll(aggrType.getColumnsNeedDictionary(measure));
        }
        return result;
    }

}

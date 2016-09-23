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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import javax.annotation.Nullable;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeDesc extends RootPersistentEntity implements IEngineAware {
    private static final Logger logger = LoggerFactory.getLogger(CubeDesc.class);

    public static class CannotFilterExtendedColumnException extends RuntimeException {
        public CannotFilterExtendedColumnException(TblColRef tblColRef) {
            super(tblColRef == null ? "null" : tblColRef.getCanonicalName());
        }
    }

    public enum DeriveType {
        LOOKUP, PK_FK, EXTENDED_COLUMN
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

    private KylinConfigExt config;
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
    @JsonProperty("dictionaries")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<DictionaryDesc> dictionaries;
    @JsonProperty("rowkey")
    private RowKeyDesc rowkey;
    @JsonProperty("hbase_mapping")
    private HBaseMappingDesc hbaseMapping;
    @JsonProperty("aggregation_groups")
    private List<AggregationGroup> aggregationGroups;
    @JsonProperty("signature")
    private String signature;
    @JsonProperty("notify_list")
    private List<String> notifyList;
    @JsonProperty("status_need_notify")
    private List<String> statusNeedNotify = Collections.emptyList();

    @JsonProperty("partition_date_start")
    private long partitionDateStart = 0L;
    @JsonProperty("partition_date_end")
    private long partitionDateEnd = 3153600000000L;
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

    private Map<TblColRef, DeriveInfo> extendedColumnToHosts = Maps.newHashMap();

    public boolean isEnableSharding() {
        //in the future may extend to other storage that is shard-able
        return storageType != IStorageAware.ID_HBASE && storageType != IStorageAware.ID_HYBRID;
    }

    public Set<TblColRef> getShardByColumns() {
        return getRowkey().getShardByColumns();
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
     * @return dimension columns excluding derived 
     */
    public List<TblColRef> listDimensionColumnsExcludingDerived(boolean alsoExcludeExtendedCol) {
        List<TblColRef> result = new ArrayList<TblColRef>();
        for (TblColRef col : dimensionColumns) {
            if (isDerived(col)) {
                continue;
            }

            if (alsoExcludeExtendedCol && isExtendedColumn(col)) {
                continue;
            }

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

    public boolean hasHostColumn(TblColRef col) {
        return isDerived(col) || isExtendedColumn(col);
    }

    public boolean isDerived(TblColRef col) {
        return derivedToHostMap.containsKey(col);
    }

    public boolean isExtendedColumn(TblColRef col) {
        return extendedColumnToHosts.containsKey(col);
    }

    public DeriveInfo getHostInfo(TblColRef derived) {
        if (isDerived(derived)) {
            return derivedToHostMap.get(derived);
        } else if (isExtendedColumn(derived)) {
            return extendedColumnToHosts.get(derived);
        }
        throw new RuntimeException("Cannot get host info for " + derived);
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
                if (wantedCols == null || Collections.disjoint(wantedCols, Arrays.asList(info.columns)) == false) // has any wanted columns?
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

    public KylinConfig getConfig() {
        return config;
    }

    private void setConfig(KylinConfigExt config) {
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

    public List<TableDesc> getLookupTableDescs() {
        return model.getLookupTableDescs();
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

    public List<DictionaryDesc> getDictionaries() {
        return dictionaries;
    }

    void setDictionaries(List<DictionaryDesc> dictionaries) {
        this.dictionaries = dictionaries;
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

    public LinkedHashMap<String, String> getOverrideKylinProps() {
        return overrideKylinProps;
    }

    private void setOverrideKylinProps(LinkedHashMap<String, String> overrideKylinProps) {
        this.overrideKylinProps = overrideKylinProps;
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

    /**
     * this method is to prevent malicious metadata change by checking the saved signature
     * with the calculated signature.
     * 
     * if you're comparing two cube descs, prefer to use consistentWith()
     * @return
     */
    public boolean checkSignature() {
        if (KylinVersion.getCurrentVersion().isCompatibleWith(new KylinVersion(getVersion())) && !KylinVersion.getCurrentVersion().isSignatureCompatibleWith(new KylinVersion(getVersion()))) {
            logger.info("checkSignature on {} is skipped as the its version is {} (not signature compatible but compatible) ", getName(), getVersion());
            return true;
        }

        if (StringUtils.isBlank(getSignature())) {
            return true;
        }

        String calculated = calculateSignature();
        String saved = getSignature();
        return calculated.equals(saved);
    }

    public boolean consistentWith(CubeDesc another) {
        if (another == null)
            return false;
        return this.calculateSignature().equals(another.calculateSignature());
    }

    public String calculateSignature() {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            StringBuilder sigString = new StringBuilder();
            sigString.append(this.name).append("|")//
                    .append(JsonUtil.writeValueAsString(this.modelName)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.nullStrings)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.dimensions)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.measures)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.rowkey)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.aggregationGroups)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.hbaseMapping)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.engineType)).append("|")//
                    .append(JsonUtil.writeValueAsString(this.storageType)).append("|");

            String signatureInput = sigString.toString().replaceAll("\\s+", "").toLowerCase();

            byte[] signature = md.digest(signatureInput.getBytes());
            String ret = new String(Base64.encodeBase64(signature));
            return ret;
        } catch (NoSuchAlgorithmException | JsonProcessingException e) {
            throw new RuntimeException("Failed to calculate signature");
        }
    }

    public Map<String, TblColRef> buildColumnNameAbbreviation() {
        Map<String, TblColRef> r = new CaseInsensitiveStringMap<TblColRef>();
        for (TblColRef col : listDimensionColumnsExcludingDerived(true)) {
            r.put(col.getName(), col);
        }
        return r;
    }

    public void init(KylinConfig config, Map<String, TableDesc> tables) {
        this.errors.clear();
        this.config = KylinConfigExt.createInstance(config, overrideKylinProps);

        if (this.modelName == null || this.modelName.length() == 0) {
            this.addError("The cubeDesc '" + this.getName() + "' doesn't have data model specified.");
        }

        // check if aggregation group is valid
        validate();

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
        List<TblColRef> dimCols = listDimensionColumnsExcludingDerived(true);
        if (rowkey.getRowKeyColumns().length != dimCols.size()) {
            addError("RowKey columns count (" + rowkey.getRowKeyColumns().length + ") does not match dimension columns count (" + dimCols.size() + "). ");
        }

        initDictionaryDesc();
    }

    public void validate() {
        int index = 0;

        for (AggregationGroup agg : getAggregationGroups()) {
            if (agg.getIncludes() == null) {
                logger.error("Aggregation group " + index + " 'includes' field not set");
                throw new IllegalStateException("Aggregation group " + index + " includes field not set");
            }

            if (agg.getSelectRule() == null) {
                logger.error("Aggregation group " + index + " 'select_rule' field not set");
                throw new IllegalStateException("Aggregation group " + index + " select rule field not set");
            }

            int combination = 1;
            Set<String> includeDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            getDims(includeDims, agg.getIncludes());

            Set<String> mandatoryDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            getDims(mandatoryDims, agg.getSelectRule().mandatory_dims);

            ArrayList<Set<String>> hierarchyDimsList = Lists.newArrayList();
            Set<String> hierarchyDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            getDims(hierarchyDimsList, hierarchyDims, agg.getSelectRule().hierarchy_dims);
            for (Set<String> hierarchy : hierarchyDimsList) {
                combination = combination * (hierarchy.size() + 1);
            }

            ArrayList<Set<String>> jointDimsList = Lists.newArrayList();
            Set<String> jointDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            getDims(jointDimsList, jointDims, agg.getSelectRule().joint_dims);
            if (jointDimsList.size() > 0) {
                combination = combination * (1 << jointDimsList.size());
            }

            if (!includeDims.containsAll(mandatoryDims) || !includeDims.containsAll(hierarchyDims) || !includeDims.containsAll(jointDims)) {
                logger.error("Aggregation group " + index + " Include dims not containing all the used dims");
                throw new IllegalStateException("Aggregation group " + index + " Include dims not containing all the used dims");
            }

            Set<String> normalDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            normalDims.addAll(includeDims);
            normalDims.removeAll(mandatoryDims);
            normalDims.removeAll(hierarchyDims);
            normalDims.removeAll(jointDims);

            combination = combination * (1 << normalDims.size());

            if (combination > config.getCubeAggrGroupMaxCombination()) {
                String msg = "Aggregation group " + index + " has too many combinations, use 'mandatory'/'hierarchy'/'joint' to optimize; or update 'kylin.cube.aggrgroup.max.combination' to a bigger value.";
                logger.error("Aggregation group " + index + " has " + combination + " combinations;");
                logger.error(msg);
                throw new IllegalStateException(msg);
            }

            if (CollectionUtils.containsAny(mandatoryDims, hierarchyDims)) {
                logger.warn("Aggregation group " + index + " mandatory dims overlap with hierarchy dims");
            }
            if (CollectionUtils.containsAny(mandatoryDims, jointDims)) {
                logger.warn("Aggregation group " + index + " mandatory dims overlap with joint dims");
            }

            if (CollectionUtils.containsAny(hierarchyDims, jointDims)) {
                logger.error("Aggregation group " + index + " hierarchy dims overlap with joint dims");
                throw new IllegalStateException("Aggregation group " + index + " hierarchy dims overlap with joint dims");
            }

            if (hasSingle(hierarchyDimsList)) {
                logger.error("Aggregation group " + index + " require at least 2 dims in a hierarchy");
                throw new IllegalStateException("Aggregation group " + index + " require at least 2 dims in a hierarchy");
            }
            if (hasSingle(jointDimsList)) {
                logger.error("Aggregation group " + index + " require at least 2 dims in a joint");
                throw new IllegalStateException("Aggregation group " + index + " require at least 2 dims in a joint");
            }

            if (hasOverlap(hierarchyDimsList, hierarchyDims)) {
                logger.error("Aggregation group " + index + " a dim exist in more than one hierarchy");
                throw new IllegalStateException("Aggregation group " + index + " a dim exist in more than one hierarchy");
            }
            if (hasOverlap(jointDimsList, jointDims)) {
                logger.error("Aggregation group " + index + " a dim exist in more than one joint");
                throw new IllegalStateException("Aggregation group " + index + " a dim exist in more than one joint");
            }

            index++;
        }
    }

    private void getDims(Set<String> dims, String[] stringSet) {
        if (stringSet != null) {
            for (String str : stringSet) {
                dims.add(str);
            }
        }
    }

    private void getDims(ArrayList<Set<String>> dimsList, Set<String> dims, String[][] stringSets) {
        if (stringSets != null) {
            for (String[] ss : stringSets) {
                Set<String> temp = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                for (String s : ss) {
                    temp.add(s);
                    dims.add(s);
                }
                dimsList.add(temp);
            }
        }
    }

    private boolean hasSingle(ArrayList<Set<String>> dimsList) {
        boolean hasSingle = false;
        for (Set<String> dims : dimsList) {
            if (dims.size() < 2)
                hasSingle = true;
        }
        return hasSingle;
    }

    private boolean hasOverlap(ArrayList<Set<String>> dimsList, Set<String> Dims) {
        boolean hasOverlap = false;
        int dimSize = 0;
        for (Set<String> dims : dimsList) {
            dimSize += dims.size();
        }
        if (dimSize != Dims.size())
            hasOverlap = true;
        return hasOverlap;
    }

    private void initDimensionColumns() {
        for (DimensionDesc dim : dimensions) {
            JoinDesc join = dim.getJoin();

            // init dimension columns
            ArrayList<TblColRef> dimCols = Lists.newArrayList();
            String colStrs = dim.getColumn();

            if ((colStrs == null && dim.isDerived()) || ("{FK}".equalsIgnoreCase(colStrs))) {
                // when column is omitted, special case

                for (TblColRef col : join.getForeignKeyColumns()) {
                    dimCols.add(initDimensionColRef(col));
                }
            } else {
                // normal case

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
                        initDerivedMap(new TblColRef[] { dimColArray[find] }, DeriveType.PK_FK, dim, new TblColRef[] { derivedCol }, null);
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

        Map<TblColRef, DeriveInfo> toHostMap = derivedToHostMap;
        Map<Array<TblColRef>, List<DeriveInfo>> hostToMap = hostToDerivedMap;

        Array<TblColRef> hostColArray = new Array<TblColRef>(hostCols);
        List<DeriveInfo> infoList = hostToMap.get(hostColArray);
        if (infoList == null) {
            hostToMap.put(hostColArray, infoList = new ArrayList<DeriveInfo>());
        }
        infoList.add(new DeriveInfo(type, dimension, derivedCols, false));

        for (int i = 0; i < derivedCols.length; i++) {
            TblColRef derivedCol = derivedCols[i];
            boolean isOneToOne = type == DeriveType.PK_FK || ArrayUtils.contains(hostCols, derivedCol) || (extra != null && extra[i].contains("1-1"));
            toHostMap.put(derivedCol, new DeriveInfo(type, dimension, hostCols, isOneToOne));
        }
    }

    private TblColRef initDimensionColRef(DimensionDesc dim, String colName) {
        TableDesc table = dim.getTableDesc();
        ColumnDesc col = table.findColumnByName(colName);
        if (col == null)
            throw new IllegalArgumentException("No column '" + colName + "' found in table " + table);

        TblColRef ref = col.getRef();

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
        List<TableDesc> lookupTables = getLookupTableDescs();
        for (MeasureDesc m : measures) {
            m.setName(m.getName().toUpperCase());

            if (m.getDependentMeasureRef() != null) {
                m.setDependentMeasureRef(m.getDependentMeasureRef().toUpperCase());
            }

            FunctionDesc func = m.getFunction();
            func.init(factTable, lookupTables);
            allColumns.addAll(func.getParameter().getColRefs());

            if (ExtendedColumnMeasureType.FUNC_RAW.equalsIgnoreCase(m.getFunction().getExpression())) {
                FunctionDesc functionDesc = m.getFunction();

                List<TblColRef> hosts = ExtendedColumnMeasureType.getExtendedColumnHosts(functionDesc);
                TblColRef extendedColumn = ExtendedColumnMeasureType.getExtendedColumn(functionDesc);
                initExtendedColumnMap(hosts.toArray(new TblColRef[hosts.size()]), extendedColumn);
            }
        }
    }

    private void initExtendedColumnMap(TblColRef[] hostCols, TblColRef extendedColumn) {
        extendedColumnToHosts.put(extendedColumn, new DeriveInfo(DeriveType.EXTENDED_COLUMN, null, hostCols, false));
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

    private void initDictionaryDesc() {
        if (dictionaries != null) {
            for (DictionaryDesc dictDesc : dictionaries) {
                dictDesc.init(this);
                allColumns.add(dictDesc.getColumnRef());
                if (dictDesc.getResuseColumnRef() != null) {
                    allColumns.add(dictDesc.getResuseColumnRef());
                }
            }
        }
    }

    public TblColRef getColumnByBitIndex(int bitIndex) {
        RowKeyColDesc[] rowKeyColumns = this.getRowkey().getRowKeyColumns();
        return rowKeyColumns[rowKeyColumns.length - 1 - bitIndex].getColRef();
    }

    public boolean hasMemoryHungryMeasures() {
        for (MeasureDesc measure : measures) {
            if (measure.getFunction().getMeasureType().isMemoryHungry()) {
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

    public boolean supportsLimitPushDown() {
        return getStorageType() != IStorageAware.ID_HBASE && getStorageType() != IStorageAware.ID_HYBRID;
    }

    public int getStorageType() {
        return storageType;
    }

    public void setStorageType(int storageType) {
        this.storageType = storageType;
    }

    @Override
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

    /** Get columns that have dictionary */
    public Set<TblColRef> getAllColumnsHaveDictionary() {
        Set<TblColRef> result = Sets.newLinkedHashSet();

        // dictionaries in dimensions
        for (RowKeyColDesc rowKeyColDesc : rowkey.getRowKeyColumns()) {
            TblColRef colRef = rowKeyColDesc.getColRef();
            if (rowkey.isUseDictionary(colRef)) {
                result.add(colRef);
            }
        }

        // dictionaries in measures
        for (MeasureDesc measure : measures) {
            MeasureType<?> aggrType = measure.getFunction().getMeasureType();
            result.addAll(aggrType.getColumnsNeedDictionary(measure.getFunction()));
        }

        // any additional dictionaries
        if (dictionaries != null) {
            for (DictionaryDesc dictDesc : dictionaries) {
                TblColRef col = dictDesc.getColumnRef();
                result.add(col);
            }
        }

        return result;
    }

    /** Get columns that need dictionary built on it. Note a column could reuse dictionary of another column. */
    public Set<TblColRef> getAllColumnsNeedDictionaryBuilt() {
        Set<TblColRef> result = getAllColumnsHaveDictionary();

        // remove columns that reuse other's dictionary
        if (dictionaries != null) {
            for (DictionaryDesc dictDesc : dictionaries) {
                if (dictDesc.getResuseColumnRef() != null) {
                    result.remove(dictDesc.getColumnRef());
                    result.add(dictDesc.getResuseColumnRef());
                }
            }
        }

        return result;
    }

    /** A column may reuse dictionary of another column, find the dict column, return same col if there's no reuse column*/
    public TblColRef getDictionaryReuseColumn(TblColRef col) {
        if (dictionaries == null) {
            return col;
        }
        for (DictionaryDesc dictDesc : dictionaries) {
            if (dictDesc.getColumnRef().equals(col) && dictDesc.getResuseColumnRef() != null) {
                return dictDesc.getResuseColumnRef();
            }
        }
        return col;
    }

    /** Get a column which can be used in distributing the source table */
    public TblColRef getDistributedByColumn() {
        Set<TblColRef> shardBy = getShardByColumns();
        if (shardBy != null && shardBy.size() > 0) {
            return shardBy.iterator().next();
        }

        return null;
    }

    public String getDictionaryBuilderClass(TblColRef col) {
        if (dictionaries == null)
            return null;

        for (DictionaryDesc desc : dictionaries) {
            if (desc.getBuilderClass() != null) {
                // column that reuses other's dict need not be built, thus should not reach here
                if (col.equals(desc.getColumnRef())) {
                    return desc.getBuilderClass();
                }
            }
        }
        return null;
    }

    public static CubeDesc getCopyOf(CubeDesc cubeDesc) {
        CubeDesc newCubeDesc = new CubeDesc();
        newCubeDesc.setName(cubeDesc.getName());
        newCubeDesc.setModelName(cubeDesc.getModelName());
        newCubeDesc.setDescription(cubeDesc.getDescription());
        newCubeDesc.setNullStrings(cubeDesc.getNullStrings());
        newCubeDesc.setDimensions(cubeDesc.getDimensions());
        newCubeDesc.setMeasures(cubeDesc.getMeasures());
        newCubeDesc.setDictionaries(cubeDesc.getDictionaries());
        newCubeDesc.setRowkey(cubeDesc.getRowkey());
        newCubeDesc.setHbaseMapping(cubeDesc.getHbaseMapping());
        newCubeDesc.setSignature(cubeDesc.getSignature());
        newCubeDesc.setNotifyList(cubeDesc.getNotifyList());
        newCubeDesc.setStatusNeedNotify(cubeDesc.getStatusNeedNotify());
        newCubeDesc.setAutoMergeTimeRanges(cubeDesc.getAutoMergeTimeRanges());
        newCubeDesc.setPartitionDateStart(cubeDesc.getPartitionDateStart());
        newCubeDesc.setPartitionDateEnd(cubeDesc.getPartitionDateEnd());
        newCubeDesc.setRetentionRange(cubeDesc.getRetentionRange());
        newCubeDesc.setEngineType(cubeDesc.getEngineType());
        newCubeDesc.setStorageType(cubeDesc.getStorageType());
        newCubeDesc.setAggregationGroups(cubeDesc.getAggregationGroups());
        newCubeDesc.setOverrideKylinProps(cubeDesc.getOverrideKylinProps());
        newCubeDesc.setConfig((KylinConfigExt) cubeDesc.getConfig());
        newCubeDesc.updateRandomUuid();
        return newCubeDesc;
    }

}

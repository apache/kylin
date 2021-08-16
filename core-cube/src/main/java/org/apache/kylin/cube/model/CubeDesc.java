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

import static org.apache.kylin.shaded.com.google.common.base.Preconditions.checkArgument;
import static org.apache.kylin.shaded.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.kylin.shaded.com.google.common.base.Preconditions.checkState;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.Array;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.extendedcolumn.ExtendedColumnMeasureType;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kylin.shaded.com.google.common.base.Joiner;
import org.apache.kylin.shaded.com.google.common.collect.Iterables;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

/**
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeDesc extends RootPersistentEntity implements IEngineAware {
    private static final Logger logger = LoggerFactory.getLogger(CubeDesc.class);

    // Use with care! Normally you should go to CubeDescManager and don't need this.
    public static JsonSerializer<CubeDesc> newSerializerForLowLevelAccess() {
        return new JsonSerializer<>(CubeDesc.class);
    }

    public static class CannotFilterExtendedColumnException extends RuntimeException {
        public CannotFilterExtendedColumnException(TblColRef tblColRef) {
            super(tblColRef == null ? "null" : tblColRef.getCanonicalName());
        }
    }

    public static final int MAX_ROWKEY_SIZE = 64;

    public enum DeriveType implements java.io.Serializable {
        LOOKUP, PK_FK, EXTENDED_COLUMN
    }

    public static class DeriveInfo implements java.io.Serializable {
        public DeriveType type;
        public JoinDesc join;
        public TblColRef[] columns;
        public boolean isOneToOne; // only used when ref from derived to host

        DeriveInfo(DeriveType type, JoinDesc join, TblColRef[] columns, boolean isOneToOne) {
            this.type = type;
            this.join = join;
            this.columns = columns;
            this.isOneToOne = isOneToOne;
        }

        @Override
        public String toString() {
            return "DeriveInfo [type=" + type + ", join=" + join + ", columns=" + Arrays.toString(columns)
                    + ", isOneToOne=" + isOneToOne + "]";
        }

    }

    // ============================================================================

    private KylinConfigExt config;
    private DataModelDesc model;

    @JsonProperty("name")
    private String name;
    @JsonProperty("is_draft")
    private boolean isDraft;
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
    @JsonProperty("volatile_range")
    private long volatileRange = 0;
    @JsonProperty("retention_range")
    private long retentionRange = 0;
    @JsonProperty("engine_type")
    private int engineType = IEngineAware.ID_SPARK_II;
    @JsonProperty("storage_type")
    private int storageType = IStorageAware.ID_PARQUET;
    @JsonProperty("override_kylin_properties")
    private LinkedHashMap<String, String> overrideKylinProps = new LinkedHashMap<String, String>();

    @JsonProperty("partition_offset_start")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<Integer, Long> partitionOffsetStart = Maps.newHashMap();

    @JsonProperty("cuboid_black_list")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Set<Long> cuboidBlackSet = Sets.newHashSet();

    @JsonProperty("parent_forward")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private int parentForward = 3;

    @JsonProperty("mandatory_dimension_set_list")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<Set<String>> mandatoryDimensionSetList = Collections.emptyList();

    // Error messages during resolving json metadata
    private List<String> errors = new ArrayList<String>();

    @JsonProperty("snapshot_table_desc_list")
    private List<SnapshotTableDesc> snapshotTableDescList = Collections.emptyList();

    private LinkedHashSet<TblColRef> allColumns = new LinkedHashSet<>();
    private LinkedHashSet<ColumnDesc> allColumnDescs = new LinkedHashSet<>();
    private LinkedHashSet<TblColRef> dimensionColumns = new LinkedHashSet<>();
    private Set<Long> mandatoryCuboids = new HashSet<>();

    private Map<TblColRef, DeriveInfo> derivedToHostMap = Maps.newHashMap();
    private Map<Array<TblColRef>, List<DeriveInfo>> hostToDerivedMap = Maps.newHashMap();

    private Map<TblColRef, DeriveInfo> extendedColumnToHosts = Maps.newHashMap();

    transient volatile private CuboidScheduler cuboidScheduler = null;

    @Override
    public String resourceName() {
        return name;
    }

    public boolean isEnableSharding() {
        //in the future may extend to other storage that is shard-able
        return storageType != IStorageAware.ID_HBASE && storageType != IStorageAware.ID_HYBRID;
    }

    public Set<TblColRef> getShardByColumns() {
        return getRowkey().getShardByColumns();
    }

    /**
     * @return all columns this cube can support, including derived
     */
    public Set<TblColRef> listAllColumns() {
        return allColumns == null ? null : Collections.unmodifiableSet(allColumns);
    }

    public Set<ColumnDesc> listAllColumnDescs() {
        return allColumnDescs == null ? null : Collections.unmodifiableSet(allColumnDescs);
    }

    /**
     * @return dimension columns including derived, BUT NOT measures
     */
    public Set<TblColRef> listDimensionColumnsIncludingDerived() {
        return dimensionColumns == null ? null : Collections.unmodifiableSet(dimensionColumns);
    }

    /**
     * @return derived dimension columns
     */
    public List<TblColRef> listDerivedDimensionColumns() {
        List<TblColRef> result = new ArrayList<TblColRef>();
        for (TblColRef col : dimensionColumns) {
            if (isDerived(col)) {
                result.add(col);
            }
        }
        return result;
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
        return model.findColumn(table, column);
    }

    public DimensionDesc findDimensionByTable(String lookupTableName) {
        lookupTableName = lookupTableName.toUpperCase(Locale.ROOT);
        for (DimensionDesc dim : dimensions)
            if (dim.getTableRef() != null && dim.getTableRef().getTableIdentity().equals(lookupTableName))
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

    public Map<Array<TblColRef>, List<DeriveInfo>> getHostToDerivedInfo(List<TblColRef> rowCols,
            Collection<TblColRef> wantedCols) {
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
        return concatResourcePath(resourceName());
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

    public boolean isDraft() {
        return isDraft;
    }

    public void setDraft(boolean isDraft) {
        this.isDraft = isDraft;
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

    public String[] getNullStrings() {
        return nullStrings;
    }

    public List<DimensionDesc> getDimensions() {
        return dimensions == null ? null : Collections.unmodifiableList(dimensions);
    }

    public void setDimensions(List<DimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public List<MeasureDesc> getMeasures() {
        return measures == null ? null : Collections.unmodifiableList(measures);
    }

    public void setMeasures(List<MeasureDesc> measures) {
        this.measures = measures;
    }

    public List<DictionaryDesc> getDictionaries() {
        return dictionaries == null ? null : Collections.unmodifiableList(dictionaries);
    }

    public void setDictionaries(List<DictionaryDesc> dictionaries) {
        this.dictionaries = dictionaries;
    }

    public RowKeyDesc getRowkey() {
        return rowkey;
    }

    public void setRowkey(RowKeyDesc rowkey) {
        this.rowkey = rowkey;
    }

    public List<AggregationGroup> getAggregationGroups() {
        return aggregationGroups == null ? null : Collections.unmodifiableList(aggregationGroups);
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
        return notifyList == null ? null : Collections.unmodifiableList(notifyList);
    }

    public void setNotifyList(List<String> notifyList) {
        this.notifyList = notifyList;
    }

    public List<String> getStatusNeedNotify() {
        return statusNeedNotify == null ? null : Collections.unmodifiableList(statusNeedNotify);
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

    public List<Set<String>> getMandatoryDimensionSetList() {
        return mandatoryDimensionSetList;
    }

    public void setMandatoryDimensionSetList(List<Set<String>> mandatoryDimensionSetList) {
        this.mandatoryDimensionSetList = mandatoryDimensionSetList;
    }

    public Set<Long> getMandatoryCuboids() {
        return mandatoryCuboids;
    }

    public boolean equalsRaw(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CubeDesc that = (CubeDesc) o;

        if (!Objects.equals(name, that.name))
            return false;
        if (!Objects.equals(modelName, that.modelName))
            return false;
        if (!Objects.equals(description, that.description))
            return false;
        if (!Objects.equals(dimensions, that.dimensions))
            return false;
        if (!Objects.equals(measures, that.measures))
            return false;
        if (!Objects.equals(dictionaries, that.dictionaries))
            return false;
        if (!Arrays.equals(rowkey.getRowKeyColumns(), that.rowkey.getRowKeyColumns()))
            return false;
        if (!Objects.equals(nullStrings, that.nullStrings))
            return false;
        if (!Arrays.equals(hbaseMapping.getColumnFamily(), that.hbaseMapping.getColumnFamily()))
            return false;
        if (aggregationGroups != that.aggregationGroups) {
            if (aggregationGroups == null || that.aggregationGroups == null) {
                return false;
            } else if (!IntStream.range(0, aggregationGroups.size())
                    .allMatch(i -> Arrays.equals(aggregationGroups.get(i).getIncludes(),
                            that.aggregationGroups.get(i).getIncludes())
                            && Objects.equals(aggregationGroups.get(i).getSelectRule(),
                                    that.aggregationGroups.get(i).getSelectRule()))) {
                return false;
            }
        }
        if (!Objects.equals(notifyList, that.notifyList))
            return false;
        if (!Objects.equals(statusNeedNotify, that.statusNeedNotify))
            return false;
        if (!Arrays.equals(autoMergeTimeRanges, that.autoMergeTimeRanges))
            return false;
        if (!Objects.equals(retentionRange, that.retentionRange))
            return false;
        if (!Objects.equals(engineType, that.engineType))
            return false;
        if (!Objects.equals(storageType, that.storageType))
            return false;
        if (!Objects.equals(overrideKylinProps, that.overrideKylinProps))
            return false;
        if (!Objects.equals(snapshotTableDescList, that.snapshotTableDescList))
            return false;
        if (!Objects.equals(partitionDateStart, that.partitionDateStart))
            return false;
        if (!Objects.equals(partitionDateEnd, that.partitionDateEnd))
            return false;
        if (!Objects.equals(parentForward, that.parentForward))
            return false;
        if (!Objects.equals(mandatoryDimensionSetList, that.mandatoryDimensionSetList))
            return false;
        return Objects.equals(cuboidBlackSet, that.cuboidBlackSet);
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

        return modelName.equals(cubeDesc.modelName);

    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + name.hashCode();
        result = 31 * result + model.getRootFactTable().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CubeDesc [name=" + name + "]";
    }

    /**
     * this method is to prevent malicious metadata change by checking the saved signature
     * with the calculated signature.
     * <p>
     * if you're comparing two cube descs, prefer to use consistentWith()
     *
     * @return
     */
    public boolean checkSignature() {
        if (this.getConfig().isIgnoreCubeSignatureInconsistency()) {
            logger.info("Skip checking cube signature");
            return true;
        }

        KylinVersion cubeVersion = new KylinVersion(getVersion());
        KylinVersion kylinVersion = KylinVersion.getCurrentVersion();
        if (!kylinVersion.isCompatibleWith(cubeVersion)) {
            logger.info("checkSignature on {} is skipped as the its version {} is different from kylin version {}",
                    getName(), cubeVersion, kylinVersion);
            return true;
        }

        if (kylinVersion.isCompatibleWith(cubeVersion) && !kylinVersion.isSignatureCompatibleWith(cubeVersion)) {
            logger.info(
                    "checkSignature on {} is skipped as the its version is {} (not signature compatible but compatible) ",
                    getName(), cubeVersion);
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
                    .append(JsonUtil.writeValueAsString(this.storageType)).append("|");

            if (mandatoryDimensionSetList != null && !mandatoryDimensionSetList.isEmpty()) {
                for (Set<String> mandatoryDimensionSet : mandatoryDimensionSetList) {
                    TreeSet<String> sortedSet = Sets.newTreeSet(mandatoryDimensionSet);
                    sigString.append(JsonUtil.writeValueAsString(sortedSet)).append("|");
                }
            }

            String signatureInput = sigString.toString().replaceAll("\\s+", "").toLowerCase(Locale.ROOT);

            byte[] signature = md.digest(signatureInput.getBytes(StandardCharsets.UTF_8));
            String ret = new String(Base64.encodeBase64(signature), StandardCharsets.UTF_8);
            return ret;
        } catch (NoSuchAlgorithmException | JsonProcessingException e) {
            throw new RuntimeException("Failed to calculate signature");
        }
    }

    public void deInit() {
        config = null;
        model = null;
        allColumns = new LinkedHashSet<>();
        allColumnDescs = new LinkedHashSet<>();
        dimensionColumns = new LinkedHashSet<>();
        derivedToHostMap = Maps.newHashMap();
        hostToDerivedMap = Maps.newHashMap();
        extendedColumnToHosts = Maps.newHashMap();
        cuboidBlackSet = Sets.newHashSet();
        cuboidScheduler = null;
    }

    public void init(KylinConfig config) {
        this.errors.clear();

        checkArgument(StringUtils.isNotBlank(name), "CubeDesc name is blank");
        checkArgument(StringUtils.isNotBlank(modelName), "CubeDesc (%s) has blank model name", name);

        // note CubeDesc.name == CubeInstance.name
        List<ProjectInstance> ownerPrj = ProjectManager.getInstance(config).findProjects(RealizationType.CUBE, name);

        // cube inherit the project override props
        LinkedHashMap<String, String> allOverrideProps = Maps.newLinkedHashMap(overrideKylinProps);

        if (ownerPrj.size() == 1) {
            Map<String, String> prjOverrideProps = ownerPrj.get(0).getOverrideKylinProps();
            for (Entry<String, String> entry : prjOverrideProps.entrySet()) {
                if (!overrideKylinProps.containsKey(entry.getKey())) {
                    allOverrideProps.put(entry.getKey(), entry.getValue());
                }
            }
        }

        this.config = KylinConfigExt.createInstance(config, allOverrideProps);

        checkArgument(this.rowkey.getRowKeyColumns().length <= this.config.getCubeRowkeyMaxSize(),
                "Too many rowkeys (%s) in CubeDesc, please try to reduce dimension number or adopt derived dimensions",
                this.rowkey.getRowKeyColumns().length);

        this.model = DataModelManager.getInstance(config).getDataModelDesc(modelName);
        checkNotNull(this.model, "DateModelDesc(%s) not found", modelName);

        for (DimensionDesc dim : dimensions) {
            dim.init(this);
        }

        initDimensionColumns();
        initMeasureColumns();

        rowkey.init(this);

        for (AggregationGroup agg : this.aggregationGroups) {
            agg.init(this, rowkey);
        }
        validateAggregationGroups(); // check if aggregation group is valid
        validateAggregationGroupsCombination();

        String hbaseMappingAdapterName = config.getHBaseMappingAdapter();

        if (hbaseMappingAdapterName != null) {
            try {
                Class<?> hbaseMappingAdapterClass = Class.forName(hbaseMappingAdapterName);
                Method initMethod = hbaseMappingAdapterClass.getMethod("initHBaseMapping", CubeDesc.class);
                initMethod.invoke(null, this);
                Method initMeasureReferenceToColumnFamilyMethod = hbaseMappingAdapterClass
                        .getMethod("initMeasureReferenceToColumnFamilyWithChecking", CubeDesc.class);
                initMeasureReferenceToColumnFamilyMethod.invoke(null, this);
            } catch (Exception e) {
                throw new RuntimeException("Error during adapting hbase mapping", e);
            }
        } else {
            // to be removed in Kylin 4.0
            if (hbaseMapping != null) {
                hbaseMapping.init(this);
                initMeasureReferenceToColumnFamily();
            }
        }

        // check all dimension columns are presented on rowkey
        List<TblColRef> dimCols = listDimensionColumnsExcludingDerived(true);
        checkState(rowkey.getRowKeyColumns().length == dimCols.size(),
                "RowKey columns count (%s) doesn't match dimensions columns count (%s)",
                rowkey.getRowKeyColumns().length, dimCols.size());

        initDictionaryDesc();
        amendAllColumns();

        // initialize mandatory cuboids based on mandatoryDimensionSetList
        initMandatoryCuboids();
    }

    private void initMandatoryCuboids() {
        this.mandatoryCuboids.clear();
        this.mandatoryCuboids.addAll(generateMandatoryCuboids(this.mandatoryDimensionSetList));
    }

    public Set<Long> generateMandatoryCuboids(List<Set<String>> mandatoryDimensionSetList) {
        Map<String, RowKeyColDesc> rowKeyColDescMap = Maps.newHashMap();
        for (RowKeyColDesc entry : getRowkey().getRowKeyColumns()) {
            rowKeyColDescMap.put(entry.getColumn(), entry);
        }

        Set<Long> mandatoryCuboids = Sets.newHashSetWithExpectedSize(mandatoryDimensionSetList.size());
        for (Set<String> mandatoryDimensionSet : mandatoryDimensionSetList) {
            long cuboid = 0L;
            for (String columnName : mandatoryDimensionSet) {
                TblColRef tblColRef = model.findColumn(columnName);
                RowKeyColDesc rowKeyColDesc = rowKeyColDescMap.get(tblColRef.getIdentity());
                // check if mandatory dimension set list is valid
                if (rowKeyColDesc == null) {
                    logger.warn("Column " + columnName + " in " + mandatoryDimensionSet + " does not exist");
                    throw new IllegalStateException(
                            "Column " + columnName + " in " + mandatoryDimensionSet + " does not exist");
                }
                cuboid |= 1L << rowKeyColDesc.getBitIndex();
            }
            mandatoryCuboids.add(cuboid);
        }
        return mandatoryCuboids;
    }

    public CuboidScheduler getInitialCuboidScheduler() {
        if (cuboidScheduler != null)
            return cuboidScheduler;

        synchronized (this) {
            if (cuboidScheduler == null) {
                cuboidScheduler = CuboidScheduler.getInstance(this);
            }
            return cuboidScheduler;
        }
    }

    public boolean isBlackedCuboid(long cuboidID) {
        return cuboidBlackSet.contains(cuboidID);
    }

    public void validateAggregationGroupsCombination() {
        int index = 1;

        for (AggregationGroup agg : getAggregationGroups()) {
            try {
                long combination = agg.calculateCuboidCombination();

                if (combination > config.getCubeAggrGroupMaxCombination()) {
                    String msg = "Aggregation group " + index + " of Cube Desc " + this.name
                            + " has too many combinations: " + combination
                            + ". Use 'mandatory'/'hierarchy'/'joint' to optimize; or update 'kylin.cube.aggrgroup.max-combination' to a bigger value.";
                    throw new TooManyCuboidException(msg);
                }
            } catch (TooManyCuboidException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("Unknown error while calculating cuboid number for " + //
                        "Aggregation group " + index + " of Cube Desc " + this.name, e);
            }

            index++;
        }

    }

    public void validateAggregationGroups() {
        int index = 1;

        for (AggregationGroup agg : getAggregationGroups()) {
            if (agg.getIncludes() == null) {
                logger.error("Aggregation group " + index + " 'includes' field not set");
                throw new IllegalStateException("Aggregation group " + index + " includes field not set");
            }

            if (agg.getSelectRule() == null) {
                logger.error("Aggregation group " + index + " 'select_rule' field not set");
                throw new IllegalStateException("Aggregation group " + index + " select rule field not set");
            }

            Set<String> includeDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            getDims(includeDims, agg.getIncludes());

            Set<String> mandatoryDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            getDims(mandatoryDims, agg.getSelectRule().mandatoryDims);

            ArrayList<Set<String>> hierarchyDimsList = Lists.newArrayList();
            Set<String> hierarchyDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            getDims(hierarchyDimsList, hierarchyDims, agg.getSelectRule().hierarchyDims);

            ArrayList<Set<String>> jointDimsList = Lists.newArrayList();
            Set<String> jointDims = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            getDims(jointDimsList, jointDims, agg.getSelectRule().jointDims);

            if (!includeDims.containsAll(mandatoryDims) || !includeDims.containsAll(hierarchyDims)
                    || !includeDims.containsAll(jointDims)) {
                List<String> notIncluded = Lists.newArrayList();
                final Iterable<String> all = Iterables
                        .unmodifiableIterable(Iterables.concat(mandatoryDims, hierarchyDims, jointDims));
                for (String dim : all) {
                    if (includeDims.contains(dim) == false) {
                        notIncluded.add(dim);
                    }
                }
                Collections.sort(notIncluded);
                logger.error(
                        "Aggregation group " + index + " Include dimensions not containing all the used dimensions");
                throw new IllegalStateException("Aggregation group " + index
                        + " 'includes' dimensions not include all the dimensions:" + notIncluded.toString());
            }

            if (CollectionUtils.containsAny(mandatoryDims, hierarchyDims)) {
                logger.warn("Aggregation group " + index + " mandatory dimensions overlap with hierarchy dimensions: "
                        + ensureOrder(CollectionUtils.intersection(mandatoryDims, hierarchyDims)));
            }
            if (CollectionUtils.containsAny(mandatoryDims, jointDims)) {
                logger.warn("Aggregation group " + index + " mandatory dimensions overlap with joint dimensions: "
                        + ensureOrder(CollectionUtils.intersection(mandatoryDims, jointDims)));
            }

            if (CollectionUtils.containsAny(hierarchyDims, jointDims)) {
                logger.error("Aggregation group " + index + " hierarchy dimensions overlap with joint dimensions");
                throw new IllegalStateException(
                        "Aggregation group " + index + " hierarchy dimensions overlap with joint dimensions: "
                                + ensureOrder(CollectionUtils.intersection(hierarchyDims, jointDims)));
            }

            if (hasSingleOrNone(hierarchyDimsList)) {
                logger.error("Aggregation group " + index + " require at least 2 dimensions in a hierarchy");
                throw new IllegalStateException(
                        "Aggregation group " + index + " require at least 2 dimensions in a hierarchy.");
            }
            if (hasSingleOrNone(jointDimsList)) {
                logger.error("Aggregation group " + index + " require at least 2 dimensions in a joint");
                throw new IllegalStateException(
                        "Aggregation group " + index + " require at least 2 dimensions in a joint");
            }

            Pair<Boolean, Set<String>> overlap = hasOverlap(hierarchyDimsList, hierarchyDims);
            if (overlap.getFirst() == true) {
                logger.error("Aggregation group " + index + " a dimension exist in more than one hierarchy: "
                        + ensureOrder(overlap.getSecond()));
                throw new IllegalStateException("Aggregation group " + index
                        + " a dimension exist in more than one hierarchy: " + ensureOrder(overlap.getSecond()));
            }

            overlap = hasOverlap(jointDimsList, jointDims);
            if (overlap.getFirst() == true) {
                logger.error("Aggregation group " + index + " a dimension exist in more than one joint: "
                        + ensureOrder(overlap.getSecond()));
                throw new IllegalStateException("Aggregation group " + index
                        + " a dimension exist in more than one joint: " + ensureOrder(overlap.getSecond()));
            }

            index++;
        }
    }

    public void validateNotifyList() {
        List<String> notifyList = getNotifyList();
        if (notifyList != null && !notifyList.isEmpty()) {
            EmailValidator emailValidator = EmailValidator.getInstance();
            for (String email : notifyList) {
                if (!emailValidator.isValid(email)) {
                    throw new IllegalArgumentException("Email [" + email + "] is not validation.");
                }
            }
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

    private boolean hasSingleOrNone(ArrayList<Set<String>> dimsList) {
        boolean hasSingleOrNone = false;
        for (Set<String> dims : dimsList) {
            if (dims.size() <= 1) {
                hasSingleOrNone = true;
                break;
            }
        }
        return hasSingleOrNone;
    }

    private Pair<Boolean, Set<String>> hasOverlap(ArrayList<Set<String>> dimsList, Set<String> Dims) {
        Set<String> existing = new HashSet<>();
        Set<String> overlap = new HashSet<>();
        for (Set<String> dims : dimsList) {
            if (CollectionUtils.containsAny(existing, dims)) {
                overlap.addAll(ensureOrder(CollectionUtils.intersection(existing, dims)));
            }
            existing.addAll(dims);
        }
        return new Pair<>(overlap.size() > 0, overlap);
    }

    private void initDimensionColumns() {
        for (DimensionDesc dim : dimensions) {
            JoinDesc join = dim.getJoin();

            // init dimension columns
            ArrayList<TblColRef> dimCols = Lists.newArrayList();
            String colStr = dim.getColumn();

            if ((colStr == null && dim.isDerived()) || ("{FK}".equalsIgnoreCase(colStr))) {
                // when column is omitted, special case
                for (TblColRef col : join.getForeignKeyColumns()) {
                    dimCols.add(initDimensionColRef(col));
                }
            } else {
                // normal case
                checkState(!StringUtils.isEmpty(colStr), "Dimension column must not be blank: %s", dim);
                dimCols.add(initDimensionColRef(dim, colStr));
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
                initDerivedMap(dimColArray, DeriveType.LOOKUP, join, derivedCols, derivedExtra);
            }

            if (join != null) {
                allColumns.addAll(Arrays.asList(join.getForeignKeyColumns()));
                allColumns.addAll(Arrays.asList(join.getPrimaryKeyColumns()));
            }
        }

        // PK-FK derive the other side
        Set<TblColRef> realDimensions = new HashSet<>(listDimensionColumnsExcludingDerived(true));
        for (JoinTableDesc joinTable : model.getJoinTables()) {
            JoinDesc join = joinTable.getJoin();
            int n = join.getForeignKeyColumns().length;
            for (int i = 0; i < n; i++) {
                TblColRef pk = join.getPrimaryKeyColumns()[i];
                TblColRef fk = join.getForeignKeyColumns()[i];
                if (realDimensions.contains(pk) && !realDimensions.contains(fk)) {
                    initDimensionColRef(fk);
                    initDerivedMap(new TblColRef[] { pk }, DeriveType.PK_FK, join, new TblColRef[] { fk }, null);
                } else if (realDimensions.contains(fk) && !realDimensions.contains(pk)) {
                    initDimensionColRef(pk);
                    initDerivedMap(new TblColRef[] { fk }, DeriveType.PK_FK, join, new TblColRef[] { pk }, null);
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

    private void initDerivedMap(TblColRef[] hostCols, DeriveType type, JoinDesc join, TblColRef[] derivedCols,
            String[] extra) {
        if (hostCols.length == 0 || derivedCols.length == 0)
            throw new IllegalStateException("host/derived columns must not be empty");

        // Although FK derives PK automatically, user unaware of this can declare PK as derived dimension explicitly.
        // In that case, derivedCols[] will contain a FK which is transformed from the PK by initDimensionColRef().
        // Must drop FK from derivedCols[] before continue.
        for (int i = 0; i < derivedCols.length; i++) {
            if (ArrayUtils.contains(hostCols, derivedCols[i])) {
                derivedCols = (TblColRef[]) ArrayUtils.remove(derivedCols, i);
                if (extra != null)
                    extra = (String[]) ArrayUtils.remove(extra, i);
                i--;
            }
        }

        if (derivedCols.length == 0)
            return;

        for (int i = 0; i < derivedCols.length; i++) {
            TblColRef derivedCol = derivedCols[i];
            boolean isOneToOne = type == DeriveType.PK_FK || ArrayUtils.contains(hostCols, derivedCol)
                    || (extra != null && extra[i].contains("1-1"));
            derivedToHostMap.put(derivedCol, new DeriveInfo(type, join, hostCols, isOneToOne));
        }

        Array<TblColRef> hostColArray = new Array<TblColRef>(hostCols);
        List<DeriveInfo> infoList = hostToDerivedMap.get(hostColArray);
        if (infoList == null) {
            infoList = new ArrayList<DeriveInfo>();
            hostToDerivedMap.put(hostColArray, infoList);
        }

        // Merged duplicated derived column
        List<TblColRef> whatsLeft = new ArrayList<>();
        for (TblColRef derCol : derivedCols) {
            boolean merged = false;
            for (DeriveInfo existing : infoList) {
                if (existing.type == type && existing.join.getPKSide().equals(join.getPKSide())) {
                    if (ArrayUtils.contains(existing.columns, derCol)) {
                        merged = true;
                        break;
                    }
                    if (type == DeriveType.LOOKUP) {
                        existing.columns = (TblColRef[]) ArrayUtils.add(existing.columns, derCol);
                        merged = true;
                        break;
                    }
                }
            }
            if (!merged)
                whatsLeft.add(derCol);
        }
        if (whatsLeft.size() > 0) {
            infoList.add(new DeriveInfo(type, join, whatsLeft.toArray(new TblColRef[whatsLeft.size()]),
                    false));
        }
    }

    private TblColRef initDimensionColRef(DimensionDesc dim, String colName) {
        TblColRef col = model.findColumn(dim.getTable(), colName);

        // for backward compatibility
        if (KylinVersion.isBefore200(getVersion())) {
            // always use FK instead PK, FK could be shared by more than one lookup tables
            JoinDesc join = dim.getJoin();
            if (join != null) {
                int idx = ArrayUtils.indexOf(join.getPrimaryKeyColumns(), col);
                if (idx >= 0) {
                    col = join.getForeignKeyColumns()[idx];
                }
            }
        }

        return initDimensionColRef(col);
    }

    private TblColRef initDimensionColRef(TblColRef col) {
        allColumns.add(col);
        dimensionColumns.add(col);
        return col;
    }

    @SuppressWarnings("deprecation")
    private void initMeasureColumns() {
        if (measures == null || measures.isEmpty()) {
            return;
        }

        for (MeasureDesc m : measures) {
            m.setName(m.getName().toUpperCase(Locale.ROOT));

            if (m.getDependentMeasureRef() != null) {
                m.setDependentMeasureRef(m.getDependentMeasureRef().toUpperCase(Locale.ROOT));
            }

            FunctionDesc func = m.getFunction();
            func.init(model);
            allColumns.addAll(func.getParameter().getColRefs());

            if (ExtendedColumnMeasureType.FUNC_EXTENDED_COLUMN.equalsIgnoreCase(m.getFunction().getExpression())) {
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

    public void initMeasureReferenceToColumnFamily() {
        if (measures == null || measures.size() == 0)
            return;

        Map<String, MeasureDesc> measureLookup = new HashMap<String, MeasureDesc>();
        for (MeasureDesc m : measures)
            measureLookup.put(m.getName(), m);
        Map<String, Integer> measureIndexLookup = new HashMap<String, Integer>();
        for (int i = 0; i < measures.size(); i++)
            measureIndexLookup.put(measures.get(i).getName(), i);

        BitSet checkEachMeasureExist = new BitSet();
        Set<String> measureSet = Sets.newHashSet();
        for (HBaseColumnFamilyDesc cf : getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc c : cf.getColumns()) {
                String[] colMeasureRefs = c.getMeasureRefs();
                MeasureDesc[] measureDescs = new MeasureDesc[colMeasureRefs.length];
                int[] measureIndex = new int[colMeasureRefs.length];
                int lastMeasureIndex = -1;
                for (int i = 0; i < colMeasureRefs.length; i++) {
                    measureDescs[i] = measureLookup.get(colMeasureRefs[i]);
                    checkState(measureDescs[i] != null, "measure desc at (%s) is null", i);
                    measureIndex[i] = measureIndexLookup.get(colMeasureRefs[i]);
                    checkState(measureIndex[i] >= 0, "measure index at (%s) not positive", i);

                    checkState(!measureSet.contains(colMeasureRefs[i]), "measure (%s) duplicates", colMeasureRefs[i]);
                    measureSet.add(colMeasureRefs[i]);
                    checkEachMeasureExist.set(measureIndex[i]);
                }
                c.setMeasures(measureDescs);
                c.setMeasureIndex(measureIndex);
                c.setColumnFamilyName(cf.getName());
            }
        }

        for (int i = 0; i < measures.size(); i++) {
            checkState(checkEachMeasureExist.get(i),
                    "measure (%s) does not exist in column family, or measure duplicates", measures.get(i));
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

    private void amendAllColumns() {
        // make sure all PF/FK are included, thus become exposed to calcite later
        Set<TableRef> tables = collectTablesOnJoinChain(allColumns);
        for (TableRef t : tables) {
            JoinDesc join = model.getJoinByPKSide(t);
            if (join != null) {
                allColumns.addAll(Arrays.asList(join.getForeignKeyColumns()));
                allColumns.addAll(Arrays.asList(join.getPrimaryKeyColumns()));
            }
        }

        for (TblColRef col : allColumns) {
            allColumnDescs.add(col.getColumnDesc());
        }
    }

    private Set<TableRef> collectTablesOnJoinChain(Set<TblColRef> columns) {
        Set<TableRef> result = new HashSet<>();
        for (TblColRef col : columns) {
            TableRef t = col.getTableRef();
            while (t != null) {
                result.add(t);
                JoinDesc join = model.getJoinByPKSide(t);
                t = join == null ? null : join.getFKSide();
            }
        }
        return result;
    }

    public long getVolatileRange() {
        return volatileRange;
    }

    public void setVolatileRange(long volatileRange) {
        this.volatileRange = volatileRange;
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

    public boolean isBroken() {
        return !errors.isEmpty();
    }

    public void addError(String message) {
        this.errors.add(message);
    }

    public String getErrorsAsString() {
        return Joiner.on("; ").join(errors);
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

    public Map<Integer, Long> getPartitionOffsetStart() {
        return partitionOffsetStart;
    }

    public void setPartitionOffsetStart(Map<Integer, Long> partitionOffsetStart) {
        this.partitionOffsetStart = partitionOffsetStart;
    }

    public Set<Long> getAllCuboids() {
        return getInitialCuboidScheduler().getAllCuboidIds();
    }

    public int getParentForward() {
        return parentForward;
    }

    public void setParentForward(int parentForward) {
        this.parentForward = parentForward;
    }

    /**
     * Get dimensions that have dictionary
     */
    public Set<TblColRef> getAllDimsHaveDictionary() {
        Set<TblColRef> result = Sets.newHashSet();

        for (RowKeyColDesc rowKeyColDesc : rowkey.getRowKeyColumns()) {
            TblColRef colRef = rowKeyColDesc.getColRef();
            if (rowkey.isUseDictionary(colRef)) {
                result.add(colRef);
            }
        }
        return result;
    }

    /**
     * Get columns that have dictionary
     */
    public Set<TblColRef> getAllColumnsHaveDictionary() {
        Set<TblColRef> result = Sets.newLinkedHashSet();

        // dictionaries in dimensions
        result.addAll(getAllDimsHaveDictionary());

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

        //mr - hive global dict
        if (overrideKylinProps.containsKey("kylin.dictionary.mr-hive.columns")) {
            String mrHiveDictColumns = overrideKylinProps.get("kylin.dictionary.mr-hive.columns");
            if (StringUtils.isNotEmpty(mrHiveDictColumns)) {
                String[] mrHiveDictColumnArr = mrHiveDictColumns.split(",");
                for (String dictColumn : mrHiveDictColumnArr) {
                    Iterator<TblColRef> it = result.iterator();
                    while (it.hasNext()) {
                        TblColRef colRef = it.next();
                        String aliasCol = colRef.getTableAlias() + "_" + colRef.getName();
                        if (aliasCol.equalsIgnoreCase(dictColumn)) {
                            logger.debug("Remove column {} because it has been built by MR", aliasCol);
                            it.remove();
                        }
                    }
                }
            }
        }

        return result;
    }

    /**
     * Get columns that need dictionary built on it. Note a column could reuse dictionary of another column.
     */
    public Set<TblColRef> getAllColumnsNeedDictionaryBuilt() {
        Set<TblColRef> result = getAllColumnsHaveDictionary();

        // remove columns that reuse other's dictionary
        if (dictionaries != null) {
            for (DictionaryDesc dictDesc : dictionaries) {
                if (dictDesc.getResuseColumnRef() != null) {
                    result.remove(dictDesc.getColumnRef());
                    result.add(dictDesc.getResuseColumnRef());
                }

                //tiretree global domain dic
                if (Objects.isNull(dictDesc.getResuseColumnRef()) && Objects.nonNull(dictDesc.getReuseColumn())) {
                    logger.info(
                            "tiretree global domain dic : column {} use tiretree global domain dic, reuse column {} ",
                            dictDesc.getColumnRef(), dictDesc.getReuseColumn());
                    result.remove(dictDesc.getColumnRef());
                }

            }
        }

        return result;
    }

    /**
     * get tiretree global domain dic
     *
     * @return
     */
    public List<CubeDescTiretreeGlobalDomainDictUtil.GlobalDict> listDomainDict() {
        List<CubeDescTiretreeGlobalDomainDictUtil.GlobalDict> dicts = new ArrayList<>();
        if (dictionaries != null && dictionaries.size() > 0) {
            for (DictionaryDesc dictionaryDesc : dictionaries) {
                if (dictionaryDesc.isDomain()) {
                    dicts.add(new CubeDescTiretreeGlobalDomainDictUtil.GlobalDict(dictionaryDesc.getColumnRef(),
                            dictionaryDesc.getReuseColumn(), dictionaryDesc.getCube(), dictionaryDesc.getModel()));
                }
            }
        }
        return dicts;
    }

    /**
     * A column may reuse dictionary of another column, find the dict column, return same col if there's no reuse column
     */
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

    /**
     * Get a column which can be used in distributing the source table
     */
    public TblColRef getDistributedByColumn() {
        Set<TblColRef> shardBy = getShardByColumns();
        if (shardBy != null && shardBy.size() > 0) {
            return shardBy.iterator().next();
        }

        return null;
    }

    public List<SnapshotTableDesc> getSnapshotTableDescList() {
        return snapshotTableDescList;
    }

    public void setSnapshotTableDescList(List<SnapshotTableDesc> snapshotTableDescList) {
        this.snapshotTableDescList = snapshotTableDescList;
    }

    public SnapshotTableDesc getSnapshotTableDesc(String tableName) {
        for (SnapshotTableDesc snapshotTableDesc : snapshotTableDescList) {
            if (snapshotTableDesc.getTableName().equalsIgnoreCase(tableName)) {
                return snapshotTableDesc;
            }
        }
        return null;
    }

    public boolean isGlobalSnapshotTable(String tableName) {
        SnapshotTableDesc desc = getSnapshotTableDesc(tableName);
        if (desc == null) {
            return false;
        }
        return desc.isGlobal();
    }

    public boolean isStreamingCube() {
        return getModel().getRootFactTable().getTableDesc().isStreamingTable();
    }

    /** Get a column which can be used to cluster the source table.
     * To reduce memory footprint in base cuboid for global dict */
    // TODO handle more than one ultra high cardinality columns use global dict in one cube
    TblColRef getClusteredByColumn() {
        if (getDistributedByColumn() != null) {
            return null;
        }

        if (dictionaries == null) {
            return null;
        }

        String clusterByColumn = config.getFlatHiveTableClusterByDictColumn();
        for (DictionaryDesc dictDesc : dictionaries) {
            if (dictDesc.getColumnRef().getName().equalsIgnoreCase(clusterByColumn)) {
                return dictDesc.getColumnRef();
            }
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

//    public List<TblColRef> getAllGlobalDictColumns() {
//        List<TblColRef> globalDictCols = new ArrayList<TblColRef>();
//        List<DictionaryDesc> dictionaryDescList = getDictionaries();
//
//        if (dictionaryDescList == null) {
//            return globalDictCols;
//        }
//
//        for (DictionaryDesc dictionaryDesc : dictionaryDescList) {
//            String cls = dictionaryDesc.getBuilderClass();
//            if (GlobalDictionaryBuilder.class.getName().equals(cls)
//                    || SegmentAppendTrieDictBuilder.class.getName().equals(cls))
//                globalDictCols.add(dictionaryDesc.getColumnRef());
//        }
//        return globalDictCols;
//    }

    // UHC (ultra high cardinality column): contain the ShardByColumns and the GlobalDictionaryColumns
    public List<TblColRef> getAllUHCColumns() {
        List<TblColRef> uhcColumns = new ArrayList<>();
//        uhcColumns.addAll(getAllGlobalDictColumns());
        uhcColumns.addAll(getShardByColumns());
        return uhcColumns;
    }

    public String getProject() {
        DataModelDesc modelDesc = getModel();
        if (modelDesc == null) {
            // In case the model for cube not exists in metadata
            List<ProjectInstance> ownerPrj = ProjectManager.getInstance(config).findProjects(RealizationType.CUBE,
                    name);

            if (ownerPrj.size() == 1) {
                return ownerPrj.get(0).getName();
            } else {
                throw new IllegalStateException("No project found for cube " + name);
            }
        } else {
            return getModel().getProject();
        }
    }

    public static CubeDesc getCopyOf(CubeDesc cubeDesc) {
        CubeDesc newCubeDesc = new CubeDesc();
        newCubeDesc.setName(cubeDesc.getName());
        newCubeDesc.setDraft(cubeDesc.isDraft());
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
        newCubeDesc.setVolatileRange(cubeDesc.getVolatileRange());
        newCubeDesc.setRetentionRange(cubeDesc.getRetentionRange());
        newCubeDesc.setEngineType(cubeDesc.getEngineType());
        newCubeDesc.setStorageType(cubeDesc.getStorageType());
        newCubeDesc.setAggregationGroups(cubeDesc.getAggregationGroups());
        newCubeDesc.setOverrideKylinProps(cubeDesc.getOverrideKylinProps());
        newCubeDesc.setConfig((KylinConfigExt) cubeDesc.getConfig());
        newCubeDesc.setPartitionOffsetStart(cubeDesc.getPartitionOffsetStart());
        newCubeDesc.setVersion(cubeDesc.getVersion());
        newCubeDesc.setParentForward(cubeDesc.getParentForward());
        newCubeDesc.setSnapshotTableDescList(cubeDesc.getSnapshotTableDescList());
        newCubeDesc.setMandatoryDimensionSetList(cubeDesc.getMandatoryDimensionSetList());
        newCubeDesc.updateRandomUuid();
        return newCubeDesc;
    }

    private Collection ensureOrder(Collection c) {
        TreeSet set = new TreeSet();
        for (Object o : c)
            set.add(o.toString());
        //System.out.println("set:"+set);
        return set;
    }
}

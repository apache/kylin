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

package org.apache.kylin.metadata.cube.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;

import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Range;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.TimeRange;
import org.apache.kylin.metadata.model.util.MultiPartitionUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({ "rawtypes", "FieldMayBeFinal" })
@Slf4j
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataSegment extends RootPersistentEntity implements ISegment, Serializable {

    @Getter
    @JsonProperty("model_uuid")
    private String modelUuid;
    @JsonProperty("name")
    private String name;
    @JsonProperty("create_time_utc")
    private long createTimeUTC;
    @JsonProperty("status")
    private SegmentStatusEnum status;

    @JsonProperty("segRange")
    private SegmentRange segmentRange;

    @JsonProperty("timeRange")
    private TimeRange timeRange;

    @JsonProperty("dimension_range_info_map")
    private Map<String, DimensionRangeInfo> dimensionRangeInfoMap = Maps.newHashMap();

    @JsonProperty("parameters")
    private Map<String, String> parameters;

    @JsonProperty("dictionaries")
    private Map<String, String> dictionaries; // table/column ==> dictionary resource path
    @JsonProperty("snapshots")
    private Map<String, String> snapshots; // table name ==> snapshot resource path
    @JsonProperty("last_build_time")
    private long lastBuildTime; // last segment incr build job

    @JsonProperty("source_count")
    private long sourceCount = 0; // source table records number

    @JsonProperty("source_bytes_size")
    private long sourceBytesSize = 0;

    @JsonProperty("column_source_bytes")
    private Map<String, Long> columnSourceBytes = Maps.newHashMap();

    @Getter
    @Setter
    @JsonProperty("ori_snapshot_size")
    private Map<String, Long> oriSnapshotSize = Maps.newHashMap();

    private Long storageSize = null;

    private Long storageFileCount = null;

    @JsonProperty("additionalInfo")
    private Map<String, String> additionalInfo = Maps.newLinkedHashMap();

    @JsonProperty("is_realtime_segment")
    private boolean isRealtimeSegment = false;

    // resumable flag, donn't cross building jobs
    // worked only in HDFSMeteStore
    @JsonProperty("is_snapshot_ready")
    private boolean isSnapshotReady = false;

    // resumable flag, donn't cross building jobs
    // worked only in HDFSMeteStore
    @JsonProperty("is_dict_ready")
    private boolean isDictReady = false;

    // resumable flag, donn't cross building jobs
    // worked only in HDFSMeteStore
    @JsonProperty("is_flat_table_ready")
    private boolean isFlatTableReady = false;

    // resumable flag, donn't cross building jobs
    // worked only in HDFSMeteStore
    @JsonProperty("is_fact_view_ready")
    private boolean isFactViewReady = false;

    @JsonProperty("multi_partitions")
    private List<SegmentPartition> multiPartitions = Lists.newArrayList();

    @JsonProperty("max_bucket_id")
    @Getter
    private long maxBucketId = -1L;

    // computed fields below
    // transient,generated by multiPartitionData
    @Getter
    private transient Map<Long, SegmentPartition> partitionMap = Maps.newHashMap();

    private transient volatile LayoutInfo layoutInfo;

    @Getter
    @Setter
    private Map<String, String> extraBuildOptions = Maps.newHashMap();

    private NDataflow dataflow;

    // only for deserialization
    public NDataSegment() {
    }

    public static NDataSegment empty() {
        return new NDataSegment(false);
    }

    protected NDataSegment(boolean lazyload) {
        if (lazyload) {
            throw new IllegalArgumentException("only manager which deserialize object can lazy load segmentdetail");
        }
        layoutInfo = new LayoutInfo();
    }

    @SuppressWarnings("CopyConstructorMissesField")
    public NDataSegment(NDataSegment other) {
        this.uuid = other.uuid;
        this.modelUuid = other.modelUuid;
        this.dataflow = other.dataflow;
        this.name = other.name;
        this.createTimeUTC = other.createTimeUTC;
        this.status = other.status;
        this.segmentRange = other.segmentRange;
        this.timeRange = other.timeRange;
        this.dictionaries = other.dictionaries;
        this.lastBuildTime = other.lastBuildTime;
        this.sourceCount = other.sourceCount;
        this.additionalInfo = other.additionalInfo;
        this.isSnapshotReady = other.isSnapshotReady;
        this.isDictReady = other.isDictReady;
        this.isFlatTableReady = other.isFlatTableReady;
        this.isFactViewReady = other.isFactViewReady;
        this.setMvcc(other.getMvcc());
        layoutInfo = new LayoutInfo(other.getSegDetails());
    }

    public <T extends Comparable<?>> NDataSegment(NDataflow df, SegmentRange<T> segmentRange) {
        this.modelUuid = df.getUuid();
        this.dataflow = df;
        this.segmentRange = segmentRange;
        this.name = Segments.makeSegmentName(segmentRange);
        this.createTimeUTC = System.currentTimeMillis();
        this.status = SegmentStatusEnum.NEW;
        this.layoutInfo = new LayoutInfo();
    }

    public <T extends Comparable<?>> NDataSegment(NDataflow df, SegmentRange<T> segmentRange,
                                                  Map<String, DimensionRangeInfo> dimensionRangeInfoMap) {
        this.dataflow = df;
        this.segmentRange = segmentRange;
        this.name = Segments.makeSegmentName(segmentRange);
        this.uuid = RandomUtil.randomUUIDStr();
        this.createTimeUTC = System.currentTimeMillis();
        this.status = SegmentStatusEnum.NEW;
        this.layoutInfo = new LayoutInfo();
        this.dimensionRangeInfoMap = dimensionRangeInfoMap;
    }

    public <T extends Comparable<?>> NDataSegment(NDataflow df, SegmentRange<T> segRange, String uuid) {
        this(df, segRange);
        if (!StringUtils.isEmpty(uuid)) {
            this.uuid = uuid;
        }
        Map<Long, NDataLayout> layouts = new HashMap<>();
        for (LayoutEntity layout : df.getIndexPlan().getAllLayouts()) {
            NDataLayout ly = NDataLayout.newDataLayout(df, getId(), layout.getId());
            layouts.put(ly.getLayoutId(), ly);
        }
        layoutInfo = new LayoutInfo(layouts);
    }

    void initAfterReload() {
        this.multiPartitions.forEach(partition -> {
            partition.setSegment(this);
            partitionMap.put(partition.getPartitionId(), partition);
        });

    }

    void initDataFlow() {
        if (dataflow == null) {
            dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getDataflow(modelUuid);
        }
    }

    @Override
    public KylinConfig getConfig() {
        return dataflow.getConfig();
    }

    @Override
    public boolean isOffsetCube() {
        return segmentRange instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange;
    }

    @Override
    public SegmentRange getSegRange() {
        return segmentRange;
    }

    @Override
    public SegmentRange.KafkaOffsetPartitionedSegmentRange getKSRange() {
        if (segmentRange instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange) {
            return (SegmentRange.KafkaOffsetPartitionedSegmentRange) segmentRange;
        }
        return null;
    }

    public Map<String, DimensionRangeInfo> getDimensionRangeInfoMap() {
        return dimensionRangeInfoMap;
    }

    @Override
    public TimeRange getTSRange() {
        if (timeRange != null) {
            return timeRange;
        } else if (segmentRange instanceof SegmentRange.TimePartitionedSegmentRange) {
            SegmentRange.TimePartitionedSegmentRange tsr = (SegmentRange.TimePartitionedSegmentRange) segmentRange;
            return new TimeRange(tsr.getStart(), tsr.getEnd());
        } else if (segmentRange instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange) {
            SegmentRange.KafkaOffsetPartitionedSegmentRange tsr = (SegmentRange.KafkaOffsetPartitionedSegmentRange) segmentRange;
            return new TimeRange(tsr.getStart(), tsr.getEnd(), tsr.getSourcePartitionOffsetStart(),
                    tsr.getSourcePartitionOffsetEnd());
        }
        return null;
    }

    @Override
    public Range<Long> getRange() {
        TimeRange tsRange = getTSRange();
        return Range.closedOpen(tsRange.getStart(), tsRange.getEnd());
    }

    public void setSegmentRange(SegmentRange segmentRange) {
        checkIsNotCachedAndShared();
        this.segmentRange = segmentRange;
    }

    public void setDimensionRangeInfoMap(Map<String, DimensionRangeInfo> dimensionRangeInfoMap) {
        checkIsNotCachedAndShared();
        this.dimensionRangeInfoMap = dimensionRangeInfoMap;
    }

    public void setTimeRange(TimeRange timeRange) {
        checkIsNotCachedAndShared();
        this.timeRange = timeRange;
    }

    public NDataSegDetails getSegDetails() {
        return getLayoutInfo().getSegDetails();
    }

    @Override
    public int getLayoutSize() {
        return getLayoutInfo().getLayoutSize();
    }

    @Override
    public int getEffectiveLayoutSize() {
        return (int) getLayoutInfo().getLayoutsMap().values().stream().filter(NDataLayout::filterEffectiveLayout)
                .count();
    }

    public NDataLayout getLayout(long layoutId) {
        return getLayout(layoutId, false);
    }

    public NDataLayout getLayout(long layoutId, boolean includingNonWorkingLayout) {
        if (includingNonWorkingLayout) {
            return getAllLayoutsMap().get(layoutId);
        }
        return getLayoutsMap().get(layoutId);
    }

    public Map<Long, NDataLayout> getLayoutsMap() {
        return getLayoutInfo().getLayoutsMap();
    }

    public Map<Long, NDataLayout> getAllLayoutsMap() {
        return getLayoutInfo().getAllLayoutsMap();
    }

    public Set<Long> getLayoutIds() {
        return getLayoutInfo().getLayoutIds();
    }

    public List<Long> getMultiPartitionIds() {
        return getLayoutInfo().getMultiPartitionIds();
    }

    public Long getBucketId(long cuboidId, Long partitionId) {
        return getLayoutInfo().getBucketId(cuboidId, partitionId);
    }

    public boolean isAlreadyBuilt(long layoutId) {
        return getLayoutInfo().isAlreadyBuilt(layoutId);
    }

    public LayoutInfo getLayoutInfo() {
        boolean doInit = false;
        if (layoutInfo == null) {
            synchronized (this) {
                if (layoutInfo == null) {
                    doInit = true;
                    layoutInfo = new LayoutInfo(true);
                }
            }
        }
        if (doInit) {
            setDependencies(Collections.singletonList(layoutInfo.getSegDetails()));
        }
        return layoutInfo;
    }

    private class LayoutInfo {
        // not required by spark cubing
        private NDataSegDetails segDetails;
        // not required by spark cubing
        private Map<Long, NDataLayout> allLayoutsMap = Collections.emptyMap();
        private Map<Long, NDataLayout> effectiveLayoutsMap = Collections.emptyMap();
        /**
         * for each layout, partition id -> bucket id
         */
        private Map<Long, Map<Long, Long>> partitionBucketMap = Maps.newHashMap();

        public LayoutInfo() {
            this(false);
        }

        public LayoutInfo(NDataSegDetails segDetails) {
            this.segDetails = segDetails;
        }

        public LayoutInfo(Map<Long, NDataLayout> layoutsMap) {
            this.allLayoutsMap = layoutsMap;
            this.effectiveLayoutsMap = layoutsMap.entrySet().stream()
                    .filter(entry -> NDataLayout.filterEffectiveLayout(entry.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private LayoutInfo(boolean loadDetail) {
            if (!loadDetail) {
                return;
            }
            segDetails = NDataSegDetailsManager.getInstance(getConfig(), project).getForSegment(NDataSegment.this);
            if (segDetails == null) {
                segDetails = NDataSegDetails.newSegDetails(dataflow, uuid);
            }

            setDependencies(Collections.singletonList(segDetails));

            IndexPlan indexPlan = dataflow.getIndexPlan();
            if (!indexPlan.isBroken()) {
                List<NDataLayout> filteredCuboids = segDetails.getAllLayouts().stream()
                        .filter(dataLayout -> indexPlan.getLayoutEntity(dataLayout.getLayoutId()) != null)
                        .collect(Collectors.toList());
                segDetails.setLayouts(filteredCuboids);
            }

            segDetails.setCachedAndShared(isCachedAndShared());
            List<NDataLayout> allLayouts = segDetails.getAllLayouts();
            allLayoutsMap = Maps.newHashMap();
            effectiveLayoutsMap = Maps.newHashMap();
            for (NDataLayout layout : allLayouts) {
                allLayoutsMap.put(layout.getLayoutId(), layout);
                if (NDataLayout.filterEffectiveLayout(layout)) {
                    effectiveLayoutsMap.put(layout.getLayoutId(), layout);
                    Map<Long, Long> cuboidBucketMap = Maps.newHashMap();
                    layout.getMultiPartition().forEach(dataPartition -> cuboidBucketMap
                            .put(dataPartition.getPartitionId(), dataPartition.getBucketId()));
                    partitionBucketMap.put(layout.getLayoutId(), cuboidBucketMap);
                }
            }
        }

        public int getLayoutSize() {
            return effectiveLayoutsMap.size();
        }

        public NDataLayout getLayout(long layoutId) {
            return effectiveLayoutsMap.get(layoutId);
        }

        public Map<Long, NDataLayout> getLayoutsMap() {
            return effectiveLayoutsMap;
        }

        public Map<Long, NDataLayout> getAllLayoutsMap() {
            return allLayoutsMap;
        }

        public Set<Long> getLayoutIds() {
            return effectiveLayoutsMap.keySet();
        }

        public List<Long> getMultiPartitionIds() {
            return partitionBucketMap.values().stream().map(entry -> entry.keySet()).flatMap(Set::stream).distinct()
                    .collect(Collectors.toList());
        }

        public Long getBucketId(long cuboidId, Long partitionId) {
            Map<Long, Long> cuboidBucketMap = partitionBucketMap.get(cuboidId);
            if (cuboidBucketMap == null)
                return null;
            return cuboidBucketMap.get(partitionId);
        }

        public boolean isAlreadyBuilt(long layoutId) {
            if (Objects.nonNull(effectiveLayoutsMap) && effectiveLayoutsMap.containsKey(layoutId)) {
                return effectiveLayoutsMap.get(layoutId).isReady();
            }
            return false;
        }

        public NDataSegDetails getSegDetails() {
            return segDetails;
        }

    }

    /**
     * Expose private {@link LayoutInfo} ONLY for tests!
     */
    public class LayoutInfoOnlyForTest extends LayoutInfo {

        public LayoutInfoOnlyForTest(Map<Long, NDataLayout> layoutsMap) {
            super(layoutsMap);
        }
    }

    @Override
    public NDataModel getModel() {
        return dataflow.getModel();
    }

    public IndexPlan getIndexPlan() {
        return dataflow.getIndexPlan();
    }

    @Override
    public void validate() {
        // Do nothing
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public NDataflow getDataflow() {
        return dataflow;
    }

    public void setDataflow(NDataflow df) {
        this.dataflow = df;
        if (!df.getUuid().equals(this.modelUuid)) {
            checkIsNotCachedAndShared();
            this.modelUuid = df.getUuid();
        }
    }

    public void setId(String uuid) {
        checkIsNotCachedAndShared();
        this.uuid = uuid;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        checkIsNotCachedAndShared();
        this.name = name;
    }

    @Override
    public SegmentStatusEnum getStatus() {
        return status;
    }

    public void setStatus(SegmentStatusEnum status) {
        checkIsNotCachedAndShared();
        this.status = status;
    }

    @Override
    public long getLastBuildTime() {
        return lastBuildTime;
    }

    public void setLastBuildTime(long lastBuildTime) {
        checkIsNotCachedAndShared();
        this.lastBuildTime = lastBuildTime;
    }

    public Map<String, String> getParameters() {
        if (parameters == null)
            parameters = Maps.newConcurrentMap();

        return isCachedAndShared() ? ImmutableMap.copyOf(parameters) : parameters;
    }

    public String getParameter(String key) {
        if (parameters == null) {
            return null;
        } else {
            return parameters.get(key);
        }
    }

    public void setParameters(Map<String, String> parameters) {
        checkIsNotCachedAndShared();
        this.parameters = parameters;
    }

    public void addParameter(String key, String value) {
        checkIsNotCachedAndShared();
        if (parameters == null)
            parameters = Maps.newConcurrentMap();
        parameters.put(key, value);
    }

    public Map<String, String> getDictionaries() {
        if (dictionaries == null)
            dictionaries = Maps.newConcurrentMap();

        return isCachedAndShared() ? ImmutableMap.copyOf(dictionaries) : dictionaries;
    }

    public void putDictResPath(TblColRef col, String dictResPath) {
        checkIsNotCachedAndShared();
        getDictionaries(); // touch to create
        String dictKey = col.getIdentity();
        dictionaries.put(dictKey, dictResPath);
    }

    public void setDictionaries(Map<String, String> dictionaries) {
        checkIsNotCachedAndShared();
        this.dictionaries = dictionaries;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        checkIsNotCachedAndShared();
        this.createTimeUTC = createTimeUTC;
    }

    public Map<String, String> getAdditionalInfo() {
        return isCachedAndShared() ? ImmutableMap.copyOf(additionalInfo) : additionalInfo;
    }

    public void setAdditionalInfo(Map<String, String> additionalInfo) {
        checkIsNotCachedAndShared();
        this.additionalInfo = additionalInfo;
    }

    public long getSourceCount() {
        if (CollectionUtils.isEmpty(multiPartitions)) {
            return sourceCount;
        }
        return multiPartitions.stream().mapToLong(SegmentPartition::getSourceCount).sum();
    }

    public void setSourceCount(long sourceCount) {
        checkIsNotCachedAndShared();
        this.sourceCount = sourceCount;
    }

    public long getSourceBytesSize() {
        return this.sourceBytesSize;
    }

    public void setSourceBytesSize(long sourceBytesSize) {
        checkIsNotCachedAndShared();
        this.sourceBytesSize = sourceBytesSize;
    }

    public Map<String, Long> getColumnSourceBytes() {
        if (sourceCount == 0) {
            if (!columnSourceBytes.isEmpty()) {
                log.warn("Segment[{}] sourceCount is 0, but columnSourceBytes non-empty.", getId());
            }
            return Maps.newHashMap();
        }
        return columnSourceBytes;
    }

    public void setColumnSourceBytes(Map<String, Long> columnSourceBytes) {
        this.columnSourceBytes = columnSourceBytes;
    }

    public List<SegmentPartition> getMultiPartitions() {
        return multiPartitions;
    }

    public void setMultiPartitions(List<SegmentPartition> multiPartitions) {
        checkIsNotCachedAndShared();
        this.multiPartitions = multiPartitions;
    }

    public Set<Long> getAllPartitionIds() {
        return multiPartitions.stream().map(SegmentPartition::getPartitionId).collect(Collectors.toSet());
    }

    // ============================================================================

    @Override
    public MetadataType resourceType() {
        return MetadataType.SEGMENT;
    }

    @Override
    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared())
            throw new IllegalStateException();
    }

    public long getStorageBytesSize() {
        if (storageSize == null) {
            long size = 0;
            Collection<NDataLayout> dataLayouts = getLayoutsMap().values();
            for (NDataLayout dataLayout : dataLayouts) {
                size += dataLayout.getByteSize();
            }
            storageSize = size;
        }
        return storageSize;
    }

    public long getStorageFileCount() {
        if (storageFileCount == null) {
            long fileCount = 0L;
            Collection<NDataLayout> dataLayouts = getLayoutsMap().values();
            for (NDataLayout dataLayout : dataLayouts) {
                fileCount += dataLayout.getFileCount();
            }
            storageFileCount = fileCount;
        }
        return storageFileCount;
    }

    public boolean isRealtimeSegment() {
        return isRealtimeSegment;
    }

    public void setRealtimeSegment(boolean realtimeSegment) {
        isRealtimeSegment = realtimeSegment;
    }

    public boolean isSnapshotReady() {
        return isSnapshotReady;
    }

    public void setSnapshotReady(boolean snapshotReady) {
        isSnapshotReady = snapshotReady;
    }

    public boolean isDictReady() {
        boolean forceBuild = Boolean
                .parseBoolean(extraBuildOptions.getOrDefault("job.retry.segment.force-build-dict", "false"));
        return isDictReady && !forceBuild;
    }

    public void setDictReady(boolean dictReady) {
        isDictReady = dictReady;
    }

    public boolean buildedDictColShouldRebuild() {
        return Boolean.parseBoolean(extraBuildOptions.getOrDefault("job.retry.segment.force-build-dict", "false"));
    }

    public boolean isFlatTableReady() {
        return isFlatTableReady;
    }

    public void setFlatTableReady(boolean flatTableReady) {
        isFlatTableReady = flatTableReady;
    }

    public boolean isFactViewReady() {
        return isFactViewReady;
    }

    public void setFactViewReady(boolean factViewReady) {
        isFactViewReady = factViewReady;
    }

    public SegmentPartition getPartition(long partitionId) {
        return partitionMap.get(partitionId);
    }

    public boolean isPartitionOverlap(String[] value) {
        List<String[]> newValues = Lists.newArrayList();
        newValues.add(value);
        return !findDuplicatePartitions(newValues).isEmpty();
    }

    public List<String[]> findDuplicatePartitions(List<String[]> newValues) {
        NDataModel model = this.getModel();
        Preconditions.checkState(model.isMultiPartitionModel());
        List<Long> oldPartitionIds = this.getMultiPartitions().stream().map(SegmentPartition::getPartitionId)
                .collect(Collectors.toList());
        List<String[]> oldPartitionValues = model.getMultiPartitionDesc().getPartitionValuesById(oldPartitionIds);
        return MultiPartitionUtil.findDuplicateValues(oldPartitionValues, newValues);
    }

    public long increaseBucket(long step) {
        checkIsNotCachedAndShared();
        return this.maxBucketId += step;
    }

    public void setMaxBucketId(long maxBucketId) {
        checkIsNotCachedAndShared();
        this.maxBucketId = maxBucketId;
    }

    public NDataSegment copy() {
        initDataFlow();
        return dataflow.getConfig().getManager(project, NDataSegmentManager.class).copy(this);
    }

    public NDataSegment copyForWrite() {
        initDataFlow();
        return dataflow.getConfig().getManager(project, NDataSegmentManager.class).copyForWrite(this);
    }

    public String getPartitionCondition() {
        PartitionDesc descDRP = dataflow.getModel().getPartitionDesc();
        if (Objects.isNull(descDRP) //
                || Objects.isNull(descDRP.getPartitionDateColumn()) //
                || Objects.isNull(segmentRange) //
                || segmentRange.isInfinite()) {
            log.info("No available PARTITION-CONDITION segment {}", modelUuid);
            return null;
        }

        return descDRP.getPartitionConditionBuilder() //
                .buildDateRangeCondition(descDRP, this, segmentRange);
    }

    @Override
    public int compareTo(ISegment other) {
        SegmentRange<?> x = this.getSegRange();
        return x.compareTo(other.getSegRange());
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NDataSegment other = (NDataSegment) obj;
        return uuid.equals(other.uuid);
    }

    @Override
    public String toString() {
        return "NDataSegment [" + modelUuid + "," + uuid + "," + segmentRange + "," + status + "]";
    }

    public String displayIdName() {
        return String.format(Locale.ROOT, "[uuid:%s,name:%s]", uuid, name);
    }
}

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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.MissingRootPersistentEntity;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.Setter;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings("serial")
@Slf4j
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataflow extends RootPersistentEntity implements Serializable, IRealization {
    public static final String REALIZATION_TYPE = "NCUBE";
    public static final String DATAFLOW_RESOURCE_ROOT = "/dataflow";
    private static final long EXPENSIVE_DATAFLOW_INITIALIZATION = 2_000L;

    public static NDataflow create(IndexPlan plan, RealizationStatusEnum realizationStatusEnum) {
        NDataflow df = new NDataflow();
        df.config = (KylinConfigExt) plan.getConfig();
        df.setUuid(plan.getUuid());
        df.setSegments(new Segments<>());
        df.setStatus(realizationStatusEnum);

        return df;
    }

    // ============================================================================

    @JsonIgnore
    @Setter
    private KylinConfigExt config;

    @JsonProperty("status")
    private RealizationStatusEnum status;

    @JsonProperty("last_status")
    private RealizationStatusEnum lastStatus;

    @JsonProperty("cost")
    private int cost = 50;

    @Getter
    @Setter
    @JsonProperty("query_hit_count")
    private int queryHitCount = 0;

    @Getter
    @Setter
    @JsonProperty("last_query_time")
    private long lastQueryTime = 0L;

    @Getter
    @Setter
    @JsonProperty("layout_query_hit_count")
    private Map<Long, FrequencyMap> layoutHitCount = Maps.newHashMap();

    @JsonManagedReference
    @JsonProperty("segments")
    private Segments<NDataSegment> segments = new Segments<>();

    @Getter
    @Setter
    private String project;

    // ================================================================

    public void initAfterReload(KylinConfigExt config, String project) {
        long start = System.currentTimeMillis();
        this.project = project;
        this.config = config;
        for (NDataSegment seg : segments) {
            seg.initAfterReload();
        }

        this.setDependencies(calcDependencies());
        long time = System.currentTimeMillis() - start;
        if (time > EXPENSIVE_DATAFLOW_INITIALIZATION) {
            log.debug("initialization finished for dataflow({}/{}) takes {}ms", project, uuid, time);
        }
    }

    @Override
    public List<RootPersistentEntity> calcDependencies() {
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(getId());

        return Lists.newArrayList(indexPlan != null ? indexPlan
                : new MissingRootPersistentEntity(IndexPlan.concatResourcePath(getId(), project)));
    }

    public KylinConfigExt getConfig() {
        return (KylinConfigExt) getIndexPlan().getConfig();
    }

    public NDataflow copy() {
        return NDataflowManager.getInstance(config, project).copy(this);
    }

    @Override
    public String resourceName() {
        return uuid;
    }

    @Override
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return "/" + project + DATAFLOW_RESOURCE_ROOT + "/" + name + MetadataConstants.FILE_SURFIX;
    }

    public Set<String> collectPrecalculationResource() {
        Set<String> r = new LinkedHashSet<>();

        // dataflow & segments
        r.add(this.getResourcePath());
        for (NDataSegment seg : segments) {
            r.add(seg.getSegDetails().getResourcePath());
        }

        // cubing plan
        r.add(getIndexPlan().getResourcePath());

        // project & model & tables & tables exd
        r.add(getModel().getProjectInstance().getResourcePath());
        r.add(getModel().getResourcePath());
        val tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (TableRef t : getModel().getAllTables()) {
            r.add(t.getTableDesc().getResourcePath());

            if (t.getTableDesc().isKafkaTable()) {
                r.add(t.getTableDesc().getKafkaConfig().getResourcePath());
            }
            val tableExtDesc = tableMetadataManager.getTableExtIfExists(t.getTableDesc());
            if (tableExtDesc != null) {
                r.add(tableExtDesc.getResourcePath());
            }
        }

        return r;
    }

    public IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(config, project).getIndexPlan(uuid);
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments) {
        return NDataflowCapabilityChecker.check(this, prunedSegments, digest);
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments,
            List<NDataSegment> prunedStreamingSegments) {
        if (isStreaming()) {
            return isCapable(digest, prunedStreamingSegments);
        } else {
            return isCapable(digest, prunedSegments);
        }
    }

    @Override
    public boolean isStreaming() {
        return getModel().isStreaming();
    }

    @Override
    public String getType() {
        return REALIZATION_TYPE;
    }

    @Override
    public NDataModel getModel() {
        return NDataModelManager.getInstance(config, project).getDataModelDesc(uuid);
    }

    public String getModelAlias() {
        NDataModel model = getModel();
        return model == null ? null : model.getAlias();
    }

    @Override
    public Set<TblColRef> getAllColumns() {
        return getIndexPlan().listAllTblColRefs();
    }

    @Override
    public Set<ColumnDesc> getAllColumnDescs() {
        return getIndexPlan().listAllColumnDescs();
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        return Lists.newArrayList(getIndexPlan().getEffectiveDimCols().values());
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        Collection<NDataModel.Measure> measures = getIndexPlan().getEffectiveMeasures().values();
        List<MeasureDesc> result = Lists.newArrayListWithExpectedSize(measures.size());
        result.addAll(measures);
        return result;
    }

    @Override
    public List<IRealization> getRealizations() {
        return Arrays.asList(this);
    }

    @Override
    public FunctionDesc findAggrFunc(FunctionDesc aggrFunc) {
        for (MeasureDesc measure : this.getMeasures()) {
            if (measure.getFunction().equals(aggrFunc))
                return measure.getFunction();
        }
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (aggrFunc.isCountOnColumn() && kylinConfig.isReplaceColCountWithCountStar()) {
            return FunctionDesc.newCountOne();
        }
        return aggrFunc;
    }

    public List<LayoutEntity> extractReadyLayouts() {
        NDataSegment latestReadySegment = getLatestReadySegment();
        if (latestReadySegment == null) {
            return Lists.newArrayList();
        }

        List<LayoutEntity> allLayouts = getIndexPlan().getAllLayouts();
        Set<Long> readyLayoutSet = latestReadySegment.getLayoutsMap().values().stream() //
                .map(NDataLayout::getLayoutId).collect(Collectors.toSet());

        allLayouts.removeIf(layout -> !readyLayoutSet.contains(layout.getId()));
        return allLayouts;
    }

    @Override
    public boolean isReady() {
        return getStatus() == RealizationStatusEnum.ONLINE;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + getModel().getAlias() + "]";
    }

    @Override
    public long getDateRangeStart() {
        return segments.getTSStart();
    }

    @Override
    public long getDateRangeEnd() {
        return segments.getTSEnd();
    }

    public NDataSegment getSegment(String segId) {
        if (StringUtils.isBlank(segId)) {
            return null;
        }
        val segments = getSegments(Sets.newHashSet(segId));
        if (CollectionUtils.isNotEmpty(segments)) {
            Preconditions.checkState(segments.size() == 1);
            return segments.get(0);
        }
        return null;
    }

    public List<NDataSegment> getSegments(Set<String> segIds) {
        List<NDataSegment> segs = Lists.newArrayList();
        for (NDataSegment seg : segments) {
            if (segIds.contains(seg.getId())) {
                segs.add(seg);
            }
        }
        return segs;
    }

    public NDataSegment getSegmentByName(String segName) {
        for (NDataSegment seg : segments) {
            if (seg.getName().equals(segName))
                return seg;
        }
        return null;
    }

    public Segments<NDataSegment> getMergingSegments(NDataSegment mergedSegment) {
        return segments.getMergingSegments(mergedSegment);
    }

    public Segments<NDataSegment> getQueryableSegments() {
        val loadingRangeManager = NDataLoadingRangeManager.getInstance(config, project);
        val loadingRange = loadingRangeManager.getDataLoadingRange(getModel().getRootFactTableName());
        if (loadingRange == null) {
            return getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        } else {
            val querableRange = loadingRangeManager.getQuerableSegmentRange(loadingRange);
            return segments.getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING)
                    .getSegmentsByRange(querableRange);
        }
    }

    public Segments<NDataSegment> getSegments(SegmentStatusEnum... statusLst) {
        return segments.getSegments(statusLst);
    }

    public Segments<NDataSegment> getFlatSegments() {
        return segments.getFlatSegments();
    }

    public Segments<NDataSegment> calculateToBeSegments(NDataSegment newSegment) {
        return segments.calculateToBeSegments(newSegment);
    }

    public Segments<NDataSegment> getBuildingSegments() {
        return segments.getBuildingSegments();
    }

    public NDataSegment getFirstSegment() {
        List<NDataSegment> existing = getSegments();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(0);
        }
    }

    public NDataSegment getLatestReadySegment() {
        Segments<NDataSegment> readySegment = getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING);
        if (readySegment.isEmpty()) {
            return null;
        } else {
            return readySegment.get(readySegment.size() - 1);
        }
    }

    public NDataSegment getLastSegment() {
        List<NDataSegment> existing = getSegments();
        if (existing.isEmpty()) {
            return null;
        } else {
            return existing.get(existing.size() - 1);
        }
    }

    public SegmentRange getCoveredRange() {
        List<NDataSegment> segs = getFlatSegments();
        if (segs.isEmpty()) {
            return null;
        } else {
            return segs.get(0).getSegRange().coverWith(segs.get(segs.size() - 1).getSegRange());
        }
    }

    public String getSegmentHdfsPath(String segmentId) {
        String hdfsWorkingDir = KapConfig.wrap(config).getMetadataWorkingDirectory();
        return hdfsWorkingDir + getProject() + "/parquet/" + getUuid() + "/" + segmentId;
    }

    public Segments<NDataSegment> getSegmentsByRange(SegmentRange range) {
        return segments.getSegmentsByRange(range);
    }

    public List<NDataSegment> getQueryableSegmentsByRange(SegmentRange range) {
        val result = Lists.<NDataSegment> newArrayList();
        for (val seg : getQueryableSegments()) {
            if (seg.getSegRange().overlaps(range)) {
                result.add(seg);
            }
        }
        return result;
    }

    @Override
    public boolean hasPrecalculatedFields() {
        return true;
    }

    @Override
    public int getStorageType() {
        return IStorageAware.ID_NDATA_STORAGE;
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public RealizationStatusEnum getLastStatus() {
        return lastStatus;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public void setStatus(RealizationStatusEnum status) {
        checkIsNotCachedAndShared();
        if (RealizationStatusEnum.BROKEN == status && RealizationStatusEnum.BROKEN != this.status) {
            this.lastStatus = this.status;
        }
        this.status = status;
    }

    public Segments<NDataSegment> getSegments() {
        return isCachedAndShared() ? new Segments(segments) : segments;
    }

    public void setSegments(Segments<NDataSegment> segments) {
        checkIsNotCachedAndShared();

        Collections.sort(segments);
        segments.validate();

        this.segments = segments;
        // need to offline model to avoid answering query
        if (segments.isEmpty() && RealizationStatusEnum.ONLINE == this.getStatus()) {
            this.setStatus(RealizationStatusEnum.OFFLINE);
        }
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        checkIsNotCachedAndShared();
        this.cost = cost;
    }

    // ============================================================================

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + uuid.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        NDataflow other = (NDataflow) obj;
        return uuid.equals(other.uuid);
    }

    @Override
    public String toString() {
        return "NDataflow [" + getModelAlias() + "]";
    }

    public Segments getSegmentsToRemoveByRetention() {
        val segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(project, getModel().getUuid());
        if (segmentConfig.canSkipHandleRetentionSegment()) {
            return null;
        } else {
            val retentionRange = segmentConfig.getRetentionRange();
            return segments.getSegmentsToRemoveByRetention(retentionRange.getRetentionRangeType(),
                    retentionRange.getRetentionRangeNumber());
        }
    }

    public boolean checkBrokenWithRelatedInfo() {
        val dfBroken = isBroken();
        if (dfBroken) {
            return dfBroken;
        }
        val cubePlanManager = NIndexPlanManager.getInstance(config, project);
        val cubePlan = cubePlanManager.getIndexPlan(uuid);
        val cubeBroken = cubePlan == null || cubePlan.isBroken();
        if (cubeBroken) {
            return cubeBroken;
        }
        val modelManager = NDataModelManager.getInstance(config, project);
        val model = modelManager.getDataModelDesc(uuid);
        return model == null || model.isBroken();
    }

    public long getStorageBytesSize() {
        long bytesSize = 0L;
        for (val segment : getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING)) {
            bytesSize += segment.getStorageBytesSize();
        }
        return bytesSize;
    }

    public long getSourceBytesSize() {
        long bytesSize = 0L;
        for (val segment : getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING)) {
            bytesSize += segment.getSourceBytesSize() == -1 ? 0 : segment.getSourceBytesSize();
        }
        return bytesSize;
    }

    public long getLastBuildTime() {
        long lastBuildTime = 0L;
        for (val segment : getSegments(SegmentStatusEnum.READY, SegmentStatusEnum.WARNING)) {
            lastBuildTime = Math.max(lastBuildTime, segment.getLastBuildTime());
        }
        return lastBuildTime;
    }

    public long getQueryHitCount(long layoutId) {
        if (getLayoutHitCount().get(layoutId) != null) {
            return getLayoutHitCount().get(layoutId).getFrequency(project);
        }
        return 0L;
    }

    public long getByteSize(long layoutId) {
        long dataSize = 0L;
        for (NDataSegment segment : getSegments()) {
            val dataCuboid = segment.getLayout(layoutId);
            if (dataCuboid == null) {
                continue;
            }
            dataSize += dataCuboid.getByteSize();
        }
        return dataSize;
    }

    public boolean hasReadySegments() {
        return isReady() && CollectionUtils.isNotEmpty(getQueryableSegments());
    }
}

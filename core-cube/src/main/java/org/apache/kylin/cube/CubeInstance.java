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

package org.apache.kylin.cube;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.realization.CapabilityResult.CapabilityInfluence;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeInstance extends RootPersistentEntity implements IRealization, IBuildable {
    private static final int COST_WEIGHT_MEASURE = 1;
    private static final int COST_WEIGHT_DIMENSION = 10;
    private static final int COST_WEIGHT_INNER_JOIN = 100;

    public static CubeInstance create(String cubeName, CubeDesc cubeDesc) {
        CubeInstance cubeInstance = new CubeInstance();

        cubeInstance.setConfig((KylinConfigExt) cubeDesc.getConfig());
        cubeInstance.setName(cubeName);
        cubeInstance.setDescName(cubeDesc.getName());
        cubeInstance.setCreateTimeUTC(System.currentTimeMillis());
        cubeInstance.setSegments(new ArrayList<CubeSegment>());
        cubeInstance.setStatus(RealizationStatusEnum.DISABLED);
        cubeInstance.updateRandomUuid();

        return cubeInstance;
    }

    @JsonIgnore
    private KylinConfigExt config;
    @JsonProperty("name")
    private String name;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("descriptor")
    private String descName;
    // Mark cube priority for query
    @JsonProperty("cost")
    private int cost = 50;
    @JsonProperty("status")
    private RealizationStatusEnum status;

    @JsonManagedReference
    @JsonProperty("segments")
    private List<CubeSegment> segments = new ArrayList<CubeSegment>();

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    // default constructor for jackson
    public CubeInstance() {
    }

    public List<CubeSegment> getBuildingSegments() {
        List<CubeSegment> buildingSegments = new ArrayList<CubeSegment>();
        if (null != segments) {
            for (CubeSegment segment : segments) {
                if (SegmentStatusEnum.NEW == segment.getStatus() || SegmentStatusEnum.READY_PENDING == segment.getStatus()) {
                    buildingSegments.add(segment);
                }
            }
        }

        return buildingSegments;
    }

    public List<CubeSegment> getMergingSegments(CubeSegment mergedSegment) {
        LinkedList<CubeSegment> result = new LinkedList<CubeSegment>();
        if (mergedSegment == null)
            return result;

        for (CubeSegment seg : this.segments) {
            if (seg.getStatus() != SegmentStatusEnum.READY && seg.getStatus() != SegmentStatusEnum.READY_PENDING)
                continue;

            if (seg == mergedSegment)
                continue;

            if (mergedSegment.sourceOffsetContains(seg)) {
                // make sure no holes
                if (result.size() > 0 && result.getLast().getSourceOffsetEnd() != seg.getSourceOffsetStart())
                    throw new IllegalStateException("Merging segments must not have holes between " + result.getLast() + " and " + seg);

                result.add(seg);
            }
        }
        return result;
    }

    public CubeDesc getDescriptor() {
        return CubeDescManager.getInstance(config).getCubeDesc(descName);
    }

    @Override
    public DataModelDesc getDataModelDesc() {
        CubeDesc cubeDesc = this.getDescriptor();
        if (cubeDesc != null) {
            return cubeDesc.getModel();
        } else {
            return null;
        }
    }

    public boolean isReady() {
        return getStatus() == RealizationStatusEnum.READY;
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String cubeName) {
        return ResourceStore.CUBE_RESOURCE_ROOT + "/" + cubeName + ".json";
    }

    @Override
    public String toString() {
        return getCanonicalName();
    }

    // ============================================================================

    @JsonProperty("size_kb")
    public long getSizeKB() {
        long sizeKb = 0L;

        for (CubeSegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeKb += cubeSegment.getSizeKB();
        }

        return sizeKb;
    }

    @JsonProperty("input_records_count")
    public long getInputRecordCount() {
        long sizeRecordCount = 0L;

        for (CubeSegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeRecordCount += cubeSegment.getInputRecords();
        }

        return sizeRecordCount;
    }

    @JsonProperty("input_records_size")
    public long getInputRecordSize() {
        long sizeRecordSize = 0L;

        for (CubeSegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeRecordSize += cubeSegment.getInputRecordsSize();
        }

        return sizeRecordSize;
    }

    public KylinConfig getConfig() {
        return config;
    }

    void setConfig(KylinConfigExt config) {
        this.config = config;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + name + "]";
    }

    @Override
    public String getFactTable() {
        return getDescriptor().getFactTable();
    }

    public TableDesc getFactTableDesc() {
        return getDescriptor().getFactTableDesc();
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        return getDescriptor().getMeasures();
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

    public String getDescName() {
        return descName.toUpperCase();
    }

    public String getOriginDescName() {
        return descName;
    }

    public void setDescName(String descName) {
        this.descName = descName;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public void setStatus(RealizationStatusEnum status) {
        this.status = status;
    }

    public CubeSegment getFirstSegment() {
        if (this.segments == null || this.segments.size() == 0) {
            return null;
        } else {
            return this.segments.get(0);
        }
    }

    public CubeSegment getLatestReadySegment() {
        CubeSegment latest = null;
        for (int i = segments.size() - 1; i >= 0; i--) {
            CubeSegment seg = segments.get(i);
            if (seg.getStatus() != SegmentStatusEnum.READY)
                continue;
            if (latest == null || latest.getDateRangeEnd() < seg.getDateRangeEnd()) {
                latest = seg;
            }
        }
        return latest;
    }

    public List<CubeSegment> getSegments() {
        return segments;
    }

    public List<CubeSegment> getSegments(SegmentStatusEnum status) {
        List<CubeSegment> result = new ArrayList<CubeSegment>();

        for (CubeSegment segment : segments) {
            if (segment.getStatus() == status) {
                result.add(segment);
            }
        }

        return result;
    }

    public CubeSegment getSegment(String name, SegmentStatusEnum status) {
        for (CubeSegment segment : segments) {
            if ((null != segment.getName() && segment.getName().equals(name)) && (status == null || segment.getStatus() == status)) {
                return segment;
            }
        }

        return null;
    }

    public void setSegments(List<CubeSegment> segments) {
        this.segments = segments;
    }

    public CubeSegment getSegmentById(String segmentId) {
        for (CubeSegment segment : segments) {
            if (Objects.equal(segment.getUuid(), segmentId)) {
                return segment;
            }
        }
        return null;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    @Override
    public CapabilityResult isCapable(SQLDigest digest) {
        CapabilityResult result = CubeCapabilityChecker.check(this, digest);
        if (result.capable) {
            result.cost = getCost(digest);
            for (CapabilityInfluence i : result.influences) {
                result.cost *= (i.suggestCostMultiplier() == 0) ? 1.0 : i.suggestCostMultiplier();
            }
        } else {
            result.cost = -1;
        }
        return result;
    }

    public int getCost(SQLDigest digest) {
        int calculatedCost = cost;

        //the number of dimensions is not as accurate as number of row key cols
        calculatedCost += getRowKeyColumnCount() * COST_WEIGHT_DIMENSION + getMeasures().size() * COST_WEIGHT_MEASURE;

        for (LookupDesc lookupDesc : this.getDescriptor().getModel().getLookups()) {
            // more tables, more cost
            if ("inner".equals(lookupDesc.getJoin().getType())) {
                // inner join cost is bigger than left join, as it will filter some records
                calculatedCost += COST_WEIGHT_INNER_JOIN;
            }
        }

        return calculatedCost;
    }

    @Override
    public RealizationType getType() {
        return RealizationType.CUBE;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return Lists.newArrayList(getDescriptor().listAllColumns());
    }

    @Override
    public long getDateRangeStart() {
        List<CubeSegment> readySegs = getSegments(SegmentStatusEnum.READY);

        long startTime = Long.MAX_VALUE;
        for (CubeSegment seg : readySegs) {
            startTime = Math.min(startTime, seg.getDateRangeStart());
        }

        return startTime;
    }

    @Override
    public long getDateRangeEnd() {

        List<CubeSegment> readySegs = getSegments(SegmentStatusEnum.READY);

        long endTime = Long.MIN_VALUE;
        for (CubeSegment seg : readySegs) {
            endTime = Math.max(endTime, seg.getDateRangeEnd());
        }

        return endTime;
    }

    @Override
    public boolean supportsLimitPushDown() {
        return getDescriptor().supportsLimitPushDown();
    }

    public int getRowKeyColumnCount() {
        return getDescriptor().getRowkey().getRowKeyColumns().length;
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        return Lists.newArrayList(getDescriptor().listDimensionColumnsIncludingDerived());
    }

    public boolean needAutoMerge() {
        if (!this.getDescriptor().getModel().getPartitionDesc().isPartitioned())
            return false;

        return this.getDescriptor().getAutoMergeTimeRanges() != null && this.getDescriptor().getAutoMergeTimeRanges().length > 0;
    }

    @Override
    public int getSourceType() {
        return getFactTableDesc().getSourceType();
    }

    @Override
    public int getStorageType() {
        return getDescriptor().getStorageType();
    }

    @Override
    public int getEngineType() {
        return getDescriptor().getEngineType();
    }

    public static CubeInstance getCopyOf(CubeInstance cubeInstance) {
        CubeInstance newCube = new CubeInstance();
        newCube.setName(cubeInstance.getName());
        newCube.setSegments(cubeInstance.getSegments());
        newCube.setDescName(cubeInstance.getDescName());
        newCube.setConfig((KylinConfigExt) cubeInstance.getConfig());
        newCube.setStatus(cubeInstance.getStatus());
        newCube.setOwner(cubeInstance.getOwner());
        newCube.setCost(cubeInstance.getCost());
        newCube.setCreateTimeUTC(System.currentTimeMillis());
        newCube.updateRandomUuid();
        return newCube;
    }

}

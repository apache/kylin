/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.cube;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.RootPersistentEntity;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.CubePartitionDesc;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeInstance extends RootPersistentEntity {

    public static CubeInstance create(String cubeName, String projectName, CubeDesc cubeDesc) {
        CubeInstance cubeInstance = new CubeInstance();

        cubeInstance.setConfig(cubeDesc.getConfig());
        cubeInstance.setName(cubeName);
        cubeInstance.setDescName(cubeDesc.getName());
        cubeInstance.setCreateTime(formatTime(System.currentTimeMillis()));
        cubeInstance.setSegments(new ArrayList<CubeSegment>());
        cubeInstance.setStatus(CubeStatusEnum.DISABLED);
        cubeInstance.updateRandomUuid();

        return cubeInstance;
    }

    @JsonIgnore
    private KylinConfig config;
    @JsonProperty("name")
    private String name;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("version")
    private String version; // user info only, we don't do version control
    @JsonProperty("descriptor")
    private String descName;
    // Mark cube priority for query
    @JsonProperty("cost")
    private int cost = 50;
    @JsonProperty("status")
    private CubeStatusEnum status;

    @JsonManagedReference
    @JsonProperty("segments")
    private List<CubeSegment> segments = new ArrayList<CubeSegment>();

    @JsonProperty("create_time")
    private String createTime;

    public List<CubeSegment> getBuildingSegments() {
        List<CubeSegment> buildingSegments = new ArrayList<CubeSegment>();
        if (null != segments) {
            for (CubeSegment segment : segments) {
                if (CubeSegmentStatusEnum.NEW == segment.getStatus() || CubeSegmentStatusEnum.READY_PENDING == segment.getStatus()) {
                    buildingSegments.add(segment);
                }
            }
        }

        return buildingSegments;
    }

    public long getAllocatedEndDate() {
        if (null == segments || segments.size() == 0) {
            return 0;
        }

        Collections.sort(segments);

        return segments.get(segments.size() - 1).getDateRangeEnd();
    }

    public long getAllocatedStartDate() {
        if (null == segments || segments.size() == 0) {
            return 0;
        }

        Collections.sort(segments);

        return segments.get(0).getDateRangeStart();
    }

    public List<CubeSegment> getMergingSegments() {
        return this.getMergingSegments(null);
    }

    public List<CubeSegment> getMergingSegments(CubeSegment cubeSegment) {
        CubeSegment buildingSegment;
        if (cubeSegment == null) {
            List<CubeSegment> buildingSegments = getBuildingSegments();
            if (buildingSegments.size() == 0) {
                return Collections.emptyList();
            }
            buildingSegment = buildingSegments.get(0);
        } else {
            buildingSegment = cubeSegment;
        }

        List<CubeSegment> mergingSegments = new ArrayList<CubeSegment>();
        if (null != this.segments) {
            for (CubeSegment segment : this.segments) {
                if (segment.getStatus() == CubeSegmentStatusEnum.READY) {
                    if (buildingSegment.getDateRangeStart() <= segment.getDateRangeStart() && buildingSegment.getDateRangeEnd() >= segment.getDateRangeEnd()) {
                        mergingSegments.add(segment);
                    }
                }
            }
        }
        return mergingSegments;

    }

    public List<CubeSegment> getRebuildingSegments() {
        List<CubeSegment> buildingSegments = getBuildingSegments();
        if (buildingSegments.size() == 0) {
            return Collections.emptyList();
        } else {
            List<CubeSegment> rebuildingSegments = new ArrayList<CubeSegment>();
            if (null != this.segments) {
                long startDate = buildingSegments.get(0).getDateRangeStart();
                long endDate = buildingSegments.get(buildingSegments.size() - 1).getDateRangeEnd();
                for (CubeSegment segment : this.segments) {
                    if (segment.getStatus() == CubeSegmentStatusEnum.READY) {
                        if (startDate >= segment.getDateRangeStart() && startDate < segment.getDateRangeEnd() && segment.getDateRangeEnd() < endDate) {
                            rebuildingSegments.add(segment);
                            continue;
                        }
                        if (startDate <= segment.getDateRangeStart() && endDate >= segment.getDateRangeEnd()) {
                            rebuildingSegments.add(segment);
                            continue;
                        }
                    }
                }
            }

            return rebuildingSegments;
        }
    }

    public CubeDesc getDescriptor() {
        return MetadataManager.getInstance(config).getCubeDesc(descName);
    }

    public InvertedIndexDesc getInvertedIndexDesc() {
        return MetadataManager.getInstance(config).getInvertedIndexDesc(name);
    }

    public boolean isInvertedIndex() {
        return getInvertedIndexDesc() != null;
    }

    public boolean isReady() {
        return getStatus() == CubeStatusEnum.READY;
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String cubeName) {
        return ResourceStore.CUBE_RESOURCE_ROOT + "/" + cubeName + ".json";
    }

    @Override
    public String toString() {
        return "Cube [name=" + name + "]";
    }

    // ============================================================================

    @JsonProperty("size_kb")
    public long getSizeKB() {
        long sizeKb = 0L;

        for (CubeSegment cubeSegment : this.getSegments(CubeSegmentStatusEnum.READY)) {
            sizeKb += cubeSegment.getSizeKB();
        }

        return sizeKb;
    }

    @JsonProperty("source_records_count")
    public long getSourceRecordCount() {
        long sizeRecordCount = 0L;

        for (CubeSegment cubeSegment : this.getSegments(CubeSegmentStatusEnum.READY)) {
            sizeRecordCount += cubeSegment.getSourceRecords();
        }

        return sizeRecordCount;
    }

    @JsonProperty("source_records_size")
    public long getSourceRecordSize() {
        long sizeRecordSize = 0L;

        for (CubeSegment cubeSegment : this.getSegments(CubeSegmentStatusEnum.READY)) {
            sizeRecordSize += cubeSegment.getSourceRecordsSize();
        }

        return sizeRecordSize;
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

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
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

    public CubeStatusEnum getStatus() {
        return status;
    }

    public void setStatus(CubeStatusEnum status) {
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
            if (seg.getStatus() != CubeSegmentStatusEnum.READY)
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

    public List<CubeSegment> getSegments(CubeSegmentStatusEnum status) {
        List<CubeSegment> result = new ArrayList<CubeSegment>();

        for (CubeSegment segment : segments) {
            if (segment.getStatus() == status) {
                result.add(segment);
            }
        }

        return result;
    }

    public List<CubeSegment> getSegment(CubeSegmentStatusEnum status) {
        List<CubeSegment> result = Lists.newArrayList();
        for (CubeSegment segment : segments) {
            if (segment.getStatus() == status) {
                result.add(segment);
            }
        }
        return result;
    }

    public CubeSegment getSegment(String name, CubeSegmentStatusEnum status) {
        for (CubeSegment segment : segments) {
            if ((null != segment.getName() && segment.getName().equals(name)) && segment.getStatus() == status) {
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

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public long[] getDateRange() {
        List<CubeSegment> readySegments = getSegment(CubeSegmentStatusEnum.READY);
        if (readySegments.isEmpty()) {
            return new long[]{0L, 0L};
        }
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        for (CubeSegment segment : readySegments) {
            if (segment.getDateRangeStart() < start) {
                start = segment.getDateRangeStart();
            }
            if (segment.getDateRangeEnd() > end) {
                end = segment.getDateRangeEnd();
            }
        }
        return new long[]{start, end};
    }

    private boolean appendOnHll() {
        CubePartitionDesc cubePartitionDesc = getDescriptor().getCubePartitionDesc();
        if (cubePartitionDesc == null) {
            return false;
        }
        if (cubePartitionDesc.getPartitionDateColumn() == null) {
            return false;
        }
        if (cubePartitionDesc.getCubePartitionType() != CubePartitionDesc.CubePartitionType.APPEND) {
            return false;
        }
        return getDescriptor().hasHolisticCountDistinctMeasures();
    }

    public boolean appendBuildOnHllMeasure(long startDate, long endDate) {
        if (!appendOnHll()) {
            return false;
        }
        List<CubeSegment> readySegments = getSegment(CubeSegmentStatusEnum.READY);
        if (readySegments.isEmpty()) {
            return false;
        }
        for (CubeSegment readySegment: readySegments) {
            if (readySegment.getDateRangeStart() == startDate && readySegment.getDateRangeEnd() == endDate) {
                //refresh build
                return false;
            }
        }
        return true;
    }

    public boolean needMergeImmediatelyAfterBuild(CubeSegment segment) {
        if (!appendOnHll()) {
            return false;
        }
        List<CubeSegment> readySegments = getSegment(CubeSegmentStatusEnum.READY);
        if (readySegments.isEmpty()) {
            return false;
        }
        for (CubeSegment readySegment: readySegments) {
            if (readySegment.getDateRangeEnd() > segment.getDateRangeStart()) {
                //has overlap and not refresh
                return true;
            }
        }
        return false;
    }

}

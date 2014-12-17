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
package com.kylinolap.invertedindex;

import java.util.ArrayList;
import java.util.Collection;
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
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.JoinDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.metadata.realization.AbstractRealization;
import com.kylinolap.metadata.realization.DataModelRealizationType;
import com.kylinolap.metadata.realization.RealizationStatusEnum;
import com.kylinolap.metadata.realization.SegmentStatusEnum;


/**
 * @author honma
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class IIInstance extends AbstractRealization {

    public static IIInstance create(String iiName, String projectName, IIDesc iiDesc) {
        IIInstance iii = new IIInstance();

        iii.setConfig(iiDesc.getConfig());
        iii.setName(iiName);
        iii.setDescName(iiDesc.getName());
        iii.setCreateTime(formatTime(System.currentTimeMillis()));
        iii.setStatus(RealizationStatusEnum.DISABLED);
        iii.updateRandomUuid();

        return iii;
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
    private RealizationStatusEnum status;

    @JsonManagedReference
    @JsonProperty("segments")
    private List<IISegment> segments = new ArrayList<IISegment>();

    @JsonProperty("create_time")
    private String createTime;



    public List<IISegment> getBuildingSegments() {
        List<IISegment> buildingSegments = new ArrayList<IISegment>();
        if (null != segments) {
            for (IISegment segment : segments) {
                if (SegmentStatusEnum.NEW == segment.getStatus() || SegmentStatusEnum.READY_PENDING == segment.getStatus()) {
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

    public List<IISegment> getMergingSegments() {
        return this.getMergingSegments(null);
    }

    public List<IISegment> getMergingSegments(IISegment iiSegment) {
        IISegment buildingSegment;
        if (iiSegment == null) {
            List<IISegment> buildingSegments = getBuildingSegments();
            if (buildingSegments.size() == 0) {
                return Collections.emptyList();
            }
            buildingSegment = buildingSegments.get(0);
        } else {
            buildingSegment = iiSegment;
        }

        List<IISegment> mergingSegments = new ArrayList<IISegment>();
        if (null != this.segments) {
            for (IISegment segment : this.segments) {
                if (segment.getStatus() == SegmentStatusEnum.READY) {
                    if (buildingSegment.getDateRangeStart() <= segment.getDateRangeStart() && buildingSegment.getDateRangeEnd() >= segment.getDateRangeEnd()) {
                        mergingSegments.add(segment);
                    }
                }
            }
        }
        return mergingSegments;

    }

    public List<IISegment> getRebuildingSegments() {
        List<IISegment> buildingSegments = getBuildingSegments();
        if (buildingSegments.size() == 0) {
            return Collections.emptyList();
        } else {
            List<IISegment> rebuildingSegments = new ArrayList<IISegment>();
            if (null != this.segments) {
                long startDate = buildingSegments.get(0).getDateRangeStart();
                long endDate = buildingSegments.get(buildingSegments.size() - 1).getDateRangeEnd();
                for (IISegment segment : this.segments) {
                    if (segment.getStatus() == SegmentStatusEnum.READY) {
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

    public IIDesc getDescriptor() {
        return IIDescManager.getInstance(config).getIIDesc(descName);
    }


    public boolean isReady() {
        return getStatus() == RealizationStatusEnum.READY;
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public static String concatResourcePath(String cubeName) {
        return ResourceStore.II_RESOURCE_ROOT + "/" + cubeName + ".json";
    }

    @Override
    public String toString() {
        return "II [name=" + name + "]";
    }

    // ============================================================================

    @JsonProperty("size_kb")
    public long getSizeKB() {
        long sizeKb = 0L;

        for (IISegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeKb += cubeSegment.getSizeKB();
        }

        return sizeKb;
    }

    @JsonProperty("source_records_count")
    public long getSourceRecordCount() {
        long sizeRecordCount = 0L;

        for (IISegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeRecordCount += cubeSegment.getSourceRecords();
        }

        return sizeRecordCount;
    }

    @JsonProperty("source_records_size")
    public long getSourceRecordSize() {
        long sizeRecordSize = 0L;

        for (IISegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
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

    public RealizationStatusEnum getStatus() {
        return status;
    }

    public void setStatus(RealizationStatusEnum status) {
        this.status = status;
    }

    public IISegment getFirstSegment() {
        if (this.segments == null || this.segments.size() == 0) {
            return null;
        } else {
            return this.segments.get(0);
        }
    }

    public IISegment getLatestReadySegment() {
        IISegment latest = null;
        for (int i = segments.size() - 1; i >= 0; i--) {
            IISegment seg = segments.get(i);
            if (seg.getStatus() != SegmentStatusEnum.READY)
                continue;
            if (latest == null || latest.getDateRangeEnd() < seg.getDateRangeEnd()) {
                latest = seg;
            }
        }
        return latest;
    }

    public List<IISegment> getSegments() {
        return segments;
    }

    public List<IISegment> getSegments(SegmentStatusEnum status) {
        List<IISegment> result = new ArrayList<IISegment>();

        for (IISegment segment : segments) {
            if (segment.getStatus() == status) {
                result.add(segment);
            }
        }

        return result;
    }

    public List<IISegment> getSegment(SegmentStatusEnum status) {
        List<IISegment> result = Lists.newArrayList();
        for (IISegment segment : segments) {
            if (segment.getStatus() == status) {
                result.add(segment);
            }
        }
        return result;
    }

    public IISegment getSegment(String name, SegmentStatusEnum status) {
        for (IISegment segment : segments) {
            if ((null != segment.getName() && segment.getName().equals(name)) && segment.getStatus() == status) {
                return segment;
            }
        }

        return null;
    }

    public void setSegments(List<IISegment> segments) {
        this.segments = segments;
    }

    public IISegment getSegmentById(String segmentId) {
        for (IISegment segment : segments) {
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
        List<IISegment> readySegments = getSegment(SegmentStatusEnum.READY);
        if (readySegments.isEmpty()) {
            return new long[] { 0L, 0L };
        }
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        for (IISegment segment : readySegments) {
            if (segment.getDateRangeStart() < start) {
                start = segment.getDateRangeStart();
            }
            if (segment.getDateRangeEnd() > end) {
                end = segment.getDateRangeEnd();
            }
        }
        return new long[] { start, end };
    }


    @Override
    public int getCost(String factTable, Collection<JoinDesc> joins, Collection<TblColRef> allColumns, Collection<FunctionDesc> aggrFunctions) {
        return 0;
    }

    @Override
    public DataModelRealizationType getType() {
        return DataModelRealizationType.INVERTED_INDEX;
    }
}

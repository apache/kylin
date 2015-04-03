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

package org.apache.kylin.invertedindex;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.metadata.model.LookupDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metadata.realization.SQLDigest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author honma
 */
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class IIInstance extends RootPersistentEntity implements IRealization {

    public static IIInstance create(String iiName, String projectName, IIDesc iiDesc) {
        IIInstance iii = new IIInstance();

        iii.setConfig(iiDesc.getConfig());
        iii.setName(iiName);
        iii.setDescName(iiDesc.getName());
        iii.setCreateTimeUTC(System.currentTimeMillis());
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

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    private String projectName;

    private static final int COST_WEIGHT_LOOKUP_TABLE = 1;
    private static final int COST_WEIGHT_INNER_JOIN = 2;

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
        return getCanonicalName();
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

    @JsonProperty("input_records_count")
    public long getInputRecordCount() {
        long sizeRecordCount = 0L;

        for (IISegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeRecordCount += cubeSegment.getInputRecords();
        }

        return sizeRecordCount;
    }

    @JsonProperty("input_records_size")
    public long getInputRecordSize() {
        long sizeRecordSize = 0L;

        for (IISegment cubeSegment : this.getSegments(SegmentStatusEnum.READY)) {
            sizeRecordSize += cubeSegment.getInputRecordsSize();
        }

        return sizeRecordSize;
    }

    public KylinConfig getConfig() {
        return config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    @Override
    public String getCanonicalName() {
        return getType() + "[name=" + name + "]";
    }

    @Override
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
        return descName;
    }

    public void setDescName(String descName) {
        this.descName = descName;
    }

    public int getCost() {
        return cost;
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


    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }


    @Override
    public boolean isCapable(SQLDigest digest) {
        if (!digest.factTable.equalsIgnoreCase(this.getFactTable()))
            return false;

        return IICapabilityChecker.check(this, digest);
    }

    @Override
    public int getCost(SQLDigest digest) {

        int calculatedCost = cost;
        for (LookupDesc lookupDesc : this.getDescriptor().getModel().getLookups()) {
            // more tables, more cost
            calculatedCost += COST_WEIGHT_LOOKUP_TABLE;
            if ("inner".equals(lookupDesc.getJoin().getType())) {
                // inner join cost is bigger than left join, as it will filter some records
                calculatedCost += COST_WEIGHT_INNER_JOIN;
            }
        }
        return calculatedCost;
    }

    @Override
    public RealizationType getType() {
        return RealizationType.INVERTED_INDEX;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return getDescriptor().listAllColumns();
    }

    @Override
    public String getFactTable() {
        return getDescriptor().getFactTableName();
    }

    @Override
    public List<MeasureDesc> getMeasures() {
        return getDescriptor().getMeasures();
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    @Override
    public long getDateRangeStart() {
        List<IISegment> readySegs = getSegments(SegmentStatusEnum.READY);

        long startTime = Long.MAX_VALUE;
        for (IISegment seg : readySegs) {
            if (seg.getDateRangeStart() < startTime)
                startTime = seg.getDateRangeStart();
        }

        return startTime;
    }

    @Override
    public long getDateRangeEnd() {

        List<IISegment> readySegs = getSegments(SegmentStatusEnum.READY);

        long endTime = 0;
        for (IISegment seg : readySegs) {
            if (seg.getDateRangeEnd() > endTime)
                endTime = seg.getDateRangeEnd();
        }

        return endTime;
    }

    @Override
    public List<TblColRef> getAllDimensions() {
        return getDescriptor().listAllDimensions();
    }

    public void setCost(int cost) {
        this.cost = cost;
    }

}

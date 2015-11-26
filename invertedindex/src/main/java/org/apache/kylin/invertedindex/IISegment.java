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

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.dict.IDictionaryAware;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TblColRef;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.IRealizationSegment;

/**
 * @author honma
 */

// TODO: remove segment concept for II, append old hbase table
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class IISegment implements Comparable<IISegment>, IRealizationSegment {

    @JsonBackReference
    private IIInstance iiInstance;
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("name")
    private String name;
    @JsonProperty("storage_location_identifier")
    private String storageLocationIdentifier; // HTable name
    @JsonProperty("date_range_start")
    private long dateRangeStart;
    @JsonProperty("date_range_end")
    private long dateRangeEnd;
    @JsonProperty("status")
    private SegmentStatusEnum status;
    @JsonProperty("size_kb")
    private long sizeKB;
    @JsonProperty("input_records")
    private long inputRecords;
    @JsonProperty("input_records_size")
    private long inputRecordsSize;
    @JsonProperty("last_build_time")
    private long lastBuildTime;
    @JsonProperty("last_build_job_id")
    private String lastBuildJobID;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("binary_signature")
    private String binarySignature; // a hash of schema and dictionary ID,
    // used for sanity check

    private transient TableRecordInfo tableRecordInfo;

    /**
     * @param startDate
     * @param endDate
     * @return if(startDate == 0 && endDate == 0), returns "FULL_BUILD", else
     * returns "yyyyMMddHHmmss_yyyyMMddHHmmss"
     */
    public static String getSegmentName(long startDate, long endDate) {
        if (startDate == 0 && endDate == 0) {
            return "FULL_BUILD";
        }

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        return dateFormat.format(startDate) + "_" + dateFormat.format(endDate);
    }

    public IIDesc getIIDesc() {
        return getIIInstance().getDescriptor();
    }

    // ============================================================================

    @Override
    public String getUuid() {
        return uuid;
    }

    public void setUuid(String id) {
        this.uuid = id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getDateRangeStart() {
        return dateRangeStart;
    }

    public void setDateRangeStart(long dateRangeStart) {
        this.dateRangeStart = dateRangeStart;
    }

    public long getDateRangeEnd() {
        return dateRangeEnd;
    }

    public void setDateRangeEnd(long dateRangeEnd) {
        this.dateRangeEnd = dateRangeEnd;
    }

    public SegmentStatusEnum getStatus() {
        return status;
    }

    public void setStatus(SegmentStatusEnum status) {
        this.status = status;
    }

    public long getSizeKB() {
        return sizeKB;
    }

    public void setSizeKB(long sizeKB) {
        this.sizeKB = sizeKB;
    }

    public long getInputRecords() {
        return inputRecords;
    }

    public void setInputRecords(long inputRecords) {
        this.inputRecords = inputRecords;
    }

    public long getInputRecordsSize() {
        return inputRecordsSize;
    }

    public void setInputRecordsSize(long inputRecordsSize) {
        this.inputRecordsSize = inputRecordsSize;
    }

    public long getLastBuildTime() {
        return lastBuildTime;
    }

    public void setLastBuildTime(long lastBuildTime) {
        this.lastBuildTime = lastBuildTime;
    }

    public String getLastBuildJobID() {
        return lastBuildJobID;
    }

    public void setLastBuildJobID(String lastBuildJobID) {
        this.lastBuildJobID = lastBuildJobID;
    }

    public String getBinarySignature() {
        return binarySignature;
    }

    public void setBinarySignature(String binarySignature) {
        this.binarySignature = binarySignature;
    }

    public IIInstance getIIInstance() {
        return iiInstance;
    }

    public void setIIInstance(IIInstance iiInstance) {
        this.iiInstance = iiInstance;
    }

    @Override
    public String getStorageLocationIdentifier() {
        return storageLocationIdentifier;
    }

    /**
     * @param storageLocationIdentifier the storageLocationIdentifier to set
     */
    public void setStorageLocationIdentifier(String storageLocationIdentifier) {
        this.storageLocationIdentifier = storageLocationIdentifier;
    }

    @Override
    public int compareTo(IISegment other) {
        if (this.dateRangeEnd < other.dateRangeEnd) {
            return -1;
        } else if (this.dateRangeEnd > other.dateRangeEnd) {
            return 1;
        } else {
            return 0;
        }
    }

    private TableRecordInfo getTableRecordInfo() {
        if (tableRecordInfo == null)
            tableRecordInfo = new TableRecordInfo(this);
        return tableRecordInfo;
    }

    public List<TblColRef> getColumns() {
        return this.getTableRecordInfo().getColumns();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("uuid", uuid).add("create_time_utc:", createTimeUTC).add("name", name).add("last_build_job_id", lastBuildJobID).add("status", status).toString();
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    @Override
    public int getEngineType() {
        return 0;
    }

    @Override
    public int getSourceType() {
        return 0;
    }

    @Override
    public int getStorageType() {
        return 0;
    }

    @Override
    public IRealization getRealization() {
        return iiInstance;
    }

    @Override
    public IJoinedFlatTableDesc getJoinedFlatTableDesc() {
        return new IIJoinedFlatTableDesc(this.getIIDesc());
    }
}

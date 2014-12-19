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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.kylinolap.dict.ColumnDictInfo;
import com.kylinolap.dict.Dictionary;
import com.kylinolap.invertedindex.index.TableRecordInfo;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.metadata.realization.SegmentStatusEnum;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author honma
 */

//TODO: remove segment concept for II, append old hbase table
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class IISegment implements Comparable<IISegment>, ColumnDictInfo {

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
    @JsonProperty("source_records")
    private long sourceRecords;
    @JsonProperty("source_records_size")
    private long sourceRecordsSize;
    @JsonProperty("last_build_time")
    private long lastBuildTime;
    @JsonProperty("last_build_job_id")
    private String lastBuildJobID;
    @JsonProperty("create_time")
    private String createTime;

    @JsonProperty("binary_signature")
    private String binarySignature; // a hash of schema and dictionary ID,
    // used for sanity check

    @JsonProperty("dictionaries")
    private ConcurrentHashMap<String, String> dictionaries; // table/column ==> dictionary resource path

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


    public String getUuid() {
        return uuid;
    }

    public void setUuid(String id) {
        this.uuid = id;
    }

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

    public long getSourceRecords() {
        return sourceRecords;
    }

    public void setSourceRecords(long sourceRecords) {
        this.sourceRecords = sourceRecords;
    }

    public long getSourceRecordsSize() {
        return sourceRecordsSize;
    }

    public void setSourceRecordsSize(long sourceRecordsSize) {
        this.sourceRecordsSize = sourceRecordsSize;
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

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
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

    public String getStorageLocationIdentifier() {
        return storageLocationIdentifier;
    }

    public Map<String, String> getDictionaries() {
        if (dictionaries == null)
            dictionaries = new ConcurrentHashMap<String, String>();
        return dictionaries;
    }

    public Collection<String> getDictionaryPaths() {
        return getDictionaries().values();
    }


    public String getDictResPath(TblColRef col) {
        return getDictionaries().get(dictKey(col));
    }

    public void putDictResPath(TblColRef col, String dictResPath) {
        getDictionaries().put(dictKey(col), dictResPath);
    }

    private String dictKey(TblColRef col) {
        return col.getTable() + "/" + col.getName();
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((iiInstance == null) ? 0 : iiInstance.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((status == null) ? 0 : status.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IISegment other = (IISegment) obj;
        if (iiInstance == null) {
            if (other.iiInstance != null)
                return false;
        } else if (!iiInstance.equals(other.iiInstance))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (status != other.status)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("uuid", uuid)
                .add("create_time:", createTime)
                .add("name", name)
                .add("last_build_job_id", lastBuildJobID)
                .add("status", status)
                .toString();
    }

    @Override
    public int getColumnLength(TblColRef col) {
        if (tableRecordInfo == null)
            tableRecordInfo = new TableRecordInfo(this);

        int index = tableRecordInfo.findColumn(col);
        return tableRecordInfo.length(index);
    }

    @Override
    public Dictionary<?> getDictionary(TblColRef col) {
        if (tableRecordInfo == null)
            tableRecordInfo = new TableRecordInfo(this);

        int index = tableRecordInfo.findColumn(col);
        return tableRecordInfo.dict(index);
    }
}

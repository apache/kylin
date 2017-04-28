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

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeSegment implements Comparable<CubeSegment>, IBuildable, ISegment, java.io.Serializable {

    @JsonBackReference
    private CubeInstance cubeInstance;
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
    @JsonProperty("source_offset_start")
    private long sourceOffsetStart;
    @JsonProperty("source_offset_end")
    private long sourceOffsetEnd;
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
    @JsonProperty("cuboid_shard_nums")
    private Map<Long, Short> cuboidShardNums = Maps.newHashMap();
    @JsonProperty("total_shards") //it is only valid when all cuboids are squshed into some shards. like the HBASE_STORAGE case, otherwise it'll stay 0
    private int totalShards = 0;
    @JsonProperty("blackout_cuboids")
    private List<Long> blackoutCuboids = Lists.newArrayList();

    @JsonProperty("binary_signature")
    private String binarySignature; // a hash of cube schema and dictionary ID, used for sanity check

    @JsonProperty("dictionaries")
    private ConcurrentHashMap<String, String> dictionaries; // table/column ==> dictionary resource path
    @JsonProperty("snapshots")
    private ConcurrentHashMap<String, String> snapshots; // table name ==> snapshot resource path

    @JsonProperty("rowkey_stats")
    private List<Object[]> rowkeyStats = Lists.newArrayList();

    @JsonProperty("source_partition_offset_start")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<Integer, Long> sourcePartitionOffsetStart = Maps.newHashMap();

    @JsonProperty("source_partition_offset_end")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<Integer, Long> sourcePartitionOffsetEnd = Maps.newHashMap();

    @JsonProperty("additionalInfo")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> additionalInfo = new LinkedHashMap<String, String>();

    private Map<Long, Short> cuboidBaseShards = Maps.newConcurrentMap(); // cuboid id ==> base(starting) shard for this cuboid

    public CubeDesc getCubeDesc() {
        return getCubeInstance().getDescriptor();
    }

    /**
     * @param startDate
     * @param endDate
     * @return if(startDate == 0 && endDate == 0), returns "FULL_BUILD", else
     * returns "yyyyMMddHHmmss_yyyyMMddHHmmss"
     */
    public static String makeSegmentName(long startDate, long endDate, long startOffset, long endOffset) {
        if (startOffset != 0 || endOffset != 0) {
            if (startOffset == 0 && (endOffset == 0 || endOffset == Long.MAX_VALUE)) {
                return "FULL_BUILD";
            }

            return startOffset + "_" + endOffset;
        }

        // using time
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.format(startDate) + "_" + dateFormat.format(endDate);
    }

    // ============================================================================

    public KylinConfig getConfig() {
        return cubeInstance.getConfig();
    }

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

    @Override
    public DataModelDesc getModel() {
        return this.getCubeDesc().getModel();
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

    @Override
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

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public String getBinarySignature() {
        return binarySignature;
    }

    public void setBinarySignature(String binarySignature) {
        this.binarySignature = binarySignature;
    }

    public CubeInstance getCubeInstance() {
        return cubeInstance;
    }

    public void setCubeInstance(CubeInstance cubeInstance) {
        this.cubeInstance = cubeInstance;
    }

    public String getStorageLocationIdentifier() {
        return storageLocationIdentifier;
    }

    public List<Object[]> getRowkeyStats() {
        return rowkeyStats;
    }

    public Map<String, String> getDictionaries() {
        if (dictionaries == null)
            dictionaries = new ConcurrentHashMap<String, String>();
        return dictionaries;
    }

    public Map<String, String> getSnapshots() {
        if (snapshots == null)
            snapshots = new ConcurrentHashMap<String, String>();
        return snapshots;
    }

    public String getSnapshotResPath(String table) {
        return getSnapshots().get(table);
    }

    public void putSnapshotResPath(String table, String snapshotResPath) {
        getSnapshots().put(table, snapshotResPath);
    }

    public Collection<String> getDictionaryPaths() {
        return getDictionaries().values();
    }

    public Collection<String> getSnapshotPaths() {
        return getSnapshots().values();
    }

    public String getDictResPath(TblColRef col) {
        String r;
        String dictKey = col.getIdentity();
        r = getDictionaries().get(dictKey);
        
        // try Kylin v1.x dict key as well
        if (r == null) {
            String v1DictKey = col.getTable() + "/" + col.getName();
            r = getDictionaries().get(v1DictKey);
        }
        
        return r;
    }

    public void putDictResPath(TblColRef col, String dictResPath) {
        String dictKey = col.getIdentity();
        getDictionaries().put(dictKey, dictResPath);
    }

    public void setStorageLocationIdentifier(String storageLocationIdentifier) {
        this.storageLocationIdentifier = storageLocationIdentifier;
    }

    public Map<TblColRef, Dictionary<String>> buildDictionaryMap() {
        Map<TblColRef, Dictionary<String>> result = Maps.newHashMap();
        for (TblColRef col : getCubeDesc().getAllColumnsHaveDictionary()) {
            result.put(col, (Dictionary<String>) getDictionary(col));
        }
        return result;
    }

    public Dictionary<String> getDictionary(TblColRef col) {
        TblColRef reuseCol = getCubeDesc().getDictionaryReuseColumn(col);
        CubeManager cubeMgr = CubeManager.getInstance(this.getCubeInstance().getConfig());
        return cubeMgr.getDictionary(this, reuseCol);
    }

    public CubeDimEncMap getDimensionEncodingMap() {
        return new CubeDimEncMap(this);
    }

    public boolean isSourceOffsetsOn() {
        return sourceOffsetStart != 0 || sourceOffsetEnd != 0;
    }

    // date range is used in place of source offsets when offsets are missing
    public long getSourceOffsetStart() {
        return isSourceOffsetsOn() ? sourceOffsetStart : dateRangeStart;
    }

    public void setSourceOffsetStart(long sourceOffsetStart) {
        this.sourceOffsetStart = sourceOffsetStart;
    }

    // date range is used in place of source offsets when offsets are missing
    public long getSourceOffsetEnd() {
        return isSourceOffsetsOn() ? sourceOffsetEnd : dateRangeEnd;
    }

    public void setSourceOffsetEnd(long sourceOffsetEnd) {
        this.sourceOffsetEnd = sourceOffsetEnd;
    }

    // date range is used in place of source offsets when offsets are missing
    public boolean sourceOffsetOverlaps(CubeSegment seg) {
        return Segments.sourceOffsetOverlaps(this, seg);
    }

    // date range is used in place of source offsets when offsets are missing
    public boolean sourceOffsetContains(ISegment seg) {
        return Segments.sourceOffsetContains(this, seg);
    }

    @Override
    public void validate() throws IllegalStateException {
        if (cubeInstance.getDescriptor().getModel().getPartitionDesc().isPartitioned()) {
            if (!isSourceOffsetsOn() && dateRangeStart >= dateRangeEnd)
                throw new IllegalStateException("Invalid segment, dateRangeStart(" + dateRangeStart + ") must be smaller than dateRangeEnd(" + dateRangeEnd + ") in segment " + this);
            if (isSourceOffsetsOn() && sourceOffsetStart >= sourceOffsetEnd)
                throw new IllegalStateException("Invalid segment, sourceOffsetStart(" + sourceOffsetStart + ") must be smaller than sourceOffsetEnd(" + sourceOffsetEnd + ") in segment " + this);
        }
    }

    @Override
    public int compareTo(CubeSegment other) {
        long comp = this.getSourceOffsetStart() - other.getSourceOffsetStart();
        if (comp != 0)
            return comp < 0 ? -1 : 1;

        comp = this.getSourceOffsetEnd() - other.getSourceOffsetEnd();
        if (comp != 0)
            return comp < 0 ? -1 : 1;
        else
            return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((cubeInstance == null) ? 0 : cubeInstance.hashCode());
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
        CubeSegment other = (CubeSegment) obj;
        if (cubeInstance == null) {
            if (other.cubeInstance != null)
                return false;
        } else if (!cubeInstance.equals(other.cubeInstance))
            return false;
        if (uuid == null) {
            if (other.uuid != null)
                return false;
        } else if (!uuid.equals(other.uuid))
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
        return cubeInstance.getName() + "[" + name + "]";
    }

    public void setDictionaries(ConcurrentHashMap<String, String> dictionaries) {
        this.dictionaries = dictionaries;
    }

    public void setSnapshots(ConcurrentHashMap<String, String> snapshots) {
        this.snapshots = snapshots;
    }

    public String getStatisticsResourcePath() {
        return getStatisticsResourcePath(this.getCubeInstance().getName(), this.getUuid());
    }

    public static String getStatisticsResourcePath(String cubeName, String cubeSegmentId) {
        return ResourceStore.CUBE_STATISTICS_ROOT + "/" + cubeName + "/" + cubeSegmentId + ".seq";
    }

    @Override
    public int getSourceType() {
        return cubeInstance.getSourceType();
    }

    @Override
    public int getEngineType() {
        return cubeInstance.getEngineType();
    }

    @Override
    public int getStorageType() {
        return cubeInstance.getStorageType();
    }

    public boolean isEnableSharding() {
        return getCubeDesc().isEnableSharding();
    }

    public Set<TblColRef> getShardByColumns() {
        return getCubeDesc().getShardByColumns();
    }

    public int getRowKeyPreambleSize() {
        return isEnableSharding() ? RowConstants.ROWKEY_SHARD_AND_CUBOID_LEN : RowConstants.ROWKEY_CUBOIDID_LEN;
    }

    /**
     * get the number of shards where each cuboid will distribute
     *
     * @return
     */
    public Short getCuboidShardNum(Long cuboidId) {
        Short ret = this.cuboidShardNums.get(cuboidId);
        if (ret == null) {
            return 1;
        } else {
            return ret;
        }
    }

    public void setCuboidShardNums(Map<Long, Short> newCuboidShards) {
        this.cuboidShardNums = newCuboidShards;
    }

    public int getTotalShards(long cuboidId) {
        if (totalShards > 0) {
            return totalShards;
        } else {
            int ret = getCuboidShardNum(cuboidId);
            return ret;
        }
    }

    public void setTotalShards(int totalShards) {
        this.totalShards = totalShards;
    }

    public short getCuboidBaseShard(Long cuboidId) {
        if (totalShards == 0)
            return 0;

        Short ret = cuboidBaseShards.get(cuboidId);
        if (ret == null) {
            ret = ShardingHash.getShard(cuboidId, totalShards);
            cuboidBaseShards.put(cuboidId, ret);
        }

        return ret;
    }

    public List<Long> getBlackoutCuboids() {
        return this.blackoutCuboids;
    }

    public IRealization getRealization() {
        return cubeInstance;
    }

    public Map<String, String> getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(Map<String, String> additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public Map<Integer, Long> getSourcePartitionOffsetEnd() {
        return sourcePartitionOffsetEnd;
    }

    public void setSourcePartitionOffsetEnd(Map<Integer, Long> sourcePartitionOffsetEnd) {
        this.sourcePartitionOffsetEnd = sourcePartitionOffsetEnd;
    }

    public Map<Integer, Long> getSourcePartitionOffsetStart() {
        return sourcePartitionOffsetStart;
    }

    public void setSourcePartitionOffsetStart(Map<Integer, Long> sourcePartitionOffsetStart) {
        this.sourcePartitionOffsetStart = sourcePartitionOffsetStart;
    }
}

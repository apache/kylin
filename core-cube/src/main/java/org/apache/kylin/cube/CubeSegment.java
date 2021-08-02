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

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShardingHash;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.CubeDimEncMap;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IBuildable;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.ISegmentAdvisor;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class CubeSegment implements IBuildable, ISegment, Serializable {

    @JsonBackReference
    private CubeInstance cubeInstance;
    @JsonProperty("uuid")
    private String uuid;
    @JsonProperty("name")
    private String name;
    @JsonProperty("storage_location_identifier")
    private String storageLocationIdentifier;
    @JsonProperty("date_range_start")
    private long dateRangeStart;
    @JsonProperty("date_range_end")
    private long dateRangeEnd;
    @JsonProperty("source_offset_start")
    @Clarification(deprecated = true)
    private long sourceOffsetStart;
    @JsonProperty("source_offset_end")
    @Clarification(deprecated = true)
    private long sourceOffsetEnd;
    @JsonProperty("status")
    private SegmentStatusEnum status;
    @JsonProperty("size_kb")
    private long sizeKB;
    @JsonProperty("is_merged")
    private boolean isMerged;
    @JsonProperty("estimate_ratio")
    private List<Double> estimateRatio;
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
    @Clarification(deprecated = true)
    private ConcurrentHashMap<String, String> dictionaries; // table/column ==> dictionary resource path
    @JsonProperty("snapshots")
    private ConcurrentHashMap<String, String> snapshots; // table name ==> snapshot resource path

    @JsonProperty("rowkey_stats")
    private List<Object[]> rowkeyStats = Lists.newArrayList();

    @JsonProperty("source_partition_offset_start")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Clarification(deprecated = true)
    private Map<Integer, Long> sourcePartitionOffsetStart = Maps.newHashMap();

    @JsonProperty("source_partition_offset_end")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Clarification(deprecated = true)
    private Map<Integer, Long> sourcePartitionOffsetEnd = Maps.newHashMap();

    @JsonProperty("stream_source_checkpoint")
    @Clarification(deprecated = true)
    private String streamSourceCheckpoint;

    @JsonProperty("additionalInfo")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, String> additionalInfo = new LinkedHashMap<String, String>();

    @JsonProperty("dimension_range_info_map")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private Map<String, DimensionRangeInfo> dimensionRangeInfoMap = Maps.newHashMap();

    private Map<Long, Short> cuboidBaseShards = Maps.newConcurrentMap(); // cuboid id ==> base(starting) shard for this cuboid

    // lazy init
    transient volatile ISegmentAdvisor advisor = null;

    public CubeDesc getCubeDesc() {
        return getCubeInstance().getDescriptor();
    }

    public CuboidScheduler getCuboidScheduler() {
        return getCubeInstance().getCuboidScheduler();
    }

    public static String makeSegmentName(TSRange tsRange, SegmentRange segRange, DataModelDesc modelDesc) {
        if (tsRange == null && segRange == null) {
            return "FULL_BUILD";
        }

        if (segRange != null) {
            return segRange.start.v + "_" + segRange.end.v;
        }

        if (!modelDesc.isStandardPartitionedDateColumn()) {
            return tsRange.start.v + "_" + tsRange.end.v;
        }

        // using time
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        return dateFormat.format(tsRange.start.v) + "_" + dateFormat.format(tsRange.end.v);
    }

    public static Pair<Long, Long> parseSegmentName(String segmentName) {
        if ("FULL".equals(segmentName)) {
            return new Pair<>(0L, 0L);
        }
        String[] startEnd = segmentName.split("_");
        if (startEnd.length != 2) {
            throw new IllegalArgumentException("the segmentName is illegal: " + segmentName);
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        try {
            long dateRangeStart = dateFormat.parse(startEnd[0]).getTime();
            long dateRangeEnd = dateFormat.parse(startEnd[1]).getTime();
            return new Pair<>(dateRangeStart, dateRangeEnd);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid segmentName for CubeSegment, segmentName = " + segmentName);
        }
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

    public boolean isMerged() {
        return isMerged;
    }

    public void setMerged(boolean isMerged) {
        this.isMerged = isMerged;
    }

    public List<Double> getEstimateRatio() {
        return estimateRatio;
    }

    public void setEstimateRatio(List<Double> estimateRatio) {
        this.estimateRatio = estimateRatio;
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

    public void resetSnapshots() {
        snapshots = new ConcurrentHashMap<String, String>();
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

    public CubeDimEncMap getDimensionEncodingMap() {
        return new CubeDimEncMap(this);
    }

    // Hide the 4 confusing fields: dateRangeStart, dateRangeEnd, sourceOffsetStart, sourceOffsetEnd.
    // They are now managed via SegmentRange and TSRange.
    long _getDateRangeStart() {
        return dateRangeStart;
    }

    void _setDateRangeStart(long dateRangeStart) {
        this.dateRangeStart = dateRangeStart;
    }

    long _getDateRangeEnd() {
        return dateRangeEnd;
    }

    void _setDateRangeEnd(long dateRangeEnd) {
        this.dateRangeEnd = dateRangeEnd;
    }

    long _getSourceOffsetStart() {
        return sourceOffsetStart;
    }

    void _setSourceOffsetStart(long sourceOffsetStart) {
        this.sourceOffsetStart = sourceOffsetStart;
    }

    long _getSourceOffsetEnd() {
        return sourceOffsetEnd;
    }

    void _setSourceOffsetEnd(long sourceOffsetEnd) {
        this.sourceOffsetEnd = sourceOffsetEnd;
    }

    @Override
    public SegmentRange getSegRange() {
        return getAdvisor().getSegRange();
    }

    public void setSegRange(SegmentRange range) {
        getAdvisor().setSegRange(range);
    }

    @Override
    public TSRange getTSRange() {
        return getAdvisor().getTSRange();
    }

    public void setTSRange(TSRange range) {
        getAdvisor().setTSRange(range);
    }

    public boolean isOffsetCube() {
        return getAdvisor().isOffsetCube();
    }

    private ISegmentAdvisor getAdvisor() {
        if (advisor != null)
            return advisor;

        synchronized (this) {
            if (advisor == null) {
                advisor = Segments.newSegmentAdvisor(this);
            }
            return advisor;
        }
    }

    @Override
    public void validate() throws IllegalStateException {
        if (cubeInstance.getDescriptor().getModel().getPartitionDesc().isPartitioned()) {
            if (!isOffsetCube() && dateRangeStart >= dateRangeEnd)
                throw new IllegalStateException("Invalid segment, dateRangeStart(" + dateRangeStart + ") must be smaller than dateRangeEnd(" + dateRangeEnd + ") in segment " + this);
            if (isOffsetCube() && sourceOffsetStart >= sourceOffsetEnd)
                throw new IllegalStateException("Invalid segment, sourceOffsetStart(" + sourceOffsetStart + ") must be smaller than sourceOffsetEnd(" + sourceOffsetEnd + ") in segment " + this);
        }
    }

    public String getProject() {
        return getCubeDesc().getProject();
    }

    @Override
    public int compareTo(ISegment other) {
        int comp = this.getSegRange().start.compareTo(other.getSegRange().start);
        if (comp != 0)
            return comp;

        return this.getSegRange().end.compareTo(other.getSegRange().end);
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
        return getStatisticsResourcePath(this.getCubeInstance().getName(), this.getUuid(), ".seq");
    }

    public String getPreciseStatisticsResourcePath() {
        return getStatisticsResourcePath(this.getCubeInstance().getName(), this.getUuid(), "");
    }

    public static String getStatisticsResourcePath(String cubeName, String cubeSegmentId) {
        return getStatisticsResourcePath(cubeName, cubeSegmentId, ".seq");
    }

    public static String getStatisticsResourcePath(String cubeName, String cubeSegmentId, String suffix) {
        return ResourceStore.CUBE_STATISTICS_ROOT + "/" + cubeName + "/" + cubeSegmentId + suffix;
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

    public Map<Long, Short> getCuboidShardNums() {
        return this.cuboidShardNums;
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

    public Map<String, DimensionRangeInfo> getDimensionRangeInfoMap() {
        return dimensionRangeInfoMap;
    }

    public void setDimensionRangeInfoMap(Map<String, DimensionRangeInfo> dimensionRangeInfoMap) {
        this.dimensionRangeInfoMap = dimensionRangeInfoMap;
    }

    public String getStreamSourceCheckpoint() {
        return streamSourceCheckpoint;
    }

    public void setStreamSourceCheckpoint(String streamSourceCheckpoint) {
        this.streamSourceCheckpoint = streamSourceCheckpoint;
    }

    public static CubeSegment getCopyOf(CubeSegment other) {
        CubeSegment copy = new CubeSegment();
        copy.cubeInstance = other.cubeInstance;
        copy.uuid = other.uuid;
        copy.name = other.name;
        copy.storageLocationIdentifier = other.storageLocationIdentifier;
        copy.dateRangeStart = other.dateRangeStart;
        copy.dateRangeEnd = other.dateRangeEnd;
        copy.sourceOffsetStart = other.sourceOffsetStart;
        copy.sourceOffsetEnd = other.sourceOffsetEnd;
        copy.status = other.status;
        copy.sizeKB = other.sizeKB;
        copy.isMerged = other.isMerged;
        copy.estimateRatio = other.estimateRatio == null ? null : Lists.newArrayList(other.estimateRatio);
        copy.inputRecords = other.inputRecords;
        copy.inputRecordsSize = other.inputRecordsSize;
        copy.lastBuildTime = other.lastBuildTime;
        copy.lastBuildJobID = other.lastBuildJobID;
        copy.createTimeUTC = other.createTimeUTC;
        copy.cuboidShardNums.putAll(other.cuboidShardNums);
        copy.totalShards = other.totalShards;
        copy.blackoutCuboids.addAll(other.blackoutCuboids);
        copy.getDictionaries().putAll(other.getDictionaries());
        copy.getSnapshots().putAll(other.getSnapshots());
        copy.rowkeyStats.addAll(other.rowkeyStats);
        copy.sourcePartitionOffsetStart.putAll(other.sourcePartitionOffsetStart);
        copy.sourcePartitionOffsetEnd.putAll(other.sourcePartitionOffsetEnd);
        if (other.streamSourceCheckpoint != null) {
            copy.streamSourceCheckpoint = other.streamSourceCheckpoint;
        }
        copy.additionalInfo.putAll(other.additionalInfo);
        copy.dimensionRangeInfoMap = other.dimensionRangeInfoMap == null ? null
                : Maps.newHashMap(other.dimensionRangeInfoMap);
        copy.binarySignature = other.binarySignature;
        return copy;
    }
}

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfigExt;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class NDataLayout implements Serializable {

    public static NDataLayout newDataLayout(NDataflow df, String segId, long layoutId) {
        return newDataLayout(NDataSegDetails.newSegDetails(df, segId), layoutId);
    }

    public static NDataLayout newDataLayout(NDataSegDetails segDetails, long layoutId) {
        NDataLayout r = new NDataLayout();
        r.setSegDetails(segDetails);
        r.setLayoutId(layoutId);
        return r;
    }

    // ============================================================================

    /**
     * bucketID start from 20000000,20000001...
     */
    public static final long BUCKET_START_ID = 20_000_000L;

    @JsonBackReference
    private NDataSegDetails segDetails;
    @JsonProperty("layout_id")
    private long layoutId;
    // Job id must have been set during SegmentBuildExec.
    @JsonProperty("build_job_id")
    private String buildJobId;
    @JsonProperty("rows")
    private long rows;
    @JsonProperty("byte_size")
    private long byteSize;
    @JsonProperty("file_count")
    private long fileCount;
    @JsonProperty("source_rows")
    private long sourceRows;
    @JsonProperty("source_byte_size")
    private long sourceByteSize;
    // partition num may be diff with file num
    @JsonProperty("partition_num")
    private int partitionNum;

    @JsonProperty("partition_values")
    private List<String> partitionValues = new ArrayList<>();

    @JsonProperty("is_ready")
    private boolean isReady = false;

    @Getter
    @JsonProperty("create_time")
    private long createTime;

    @Getter
    @Setter
    @JsonProperty("multi_partition")
    private List<LayoutPartition> multiPartition = new ArrayList<>();

    public NDataLayout() {
        this.createTime = System.currentTimeMillis();
    }

    public KylinConfigExt getConfig() {
        return segDetails.getConfig();
    }

    public LayoutEntity getLayout() {
        return segDetails.getDataflow().getIndexPlan().getLayoutEntity(layoutId);
    }

    // ============================================================================
    // NOTE THE SPECIAL GETTERS AND SETTERS TO PROTECT CACHED OBJECTS FROM BEING MODIFIED
    // ============================================================================

    public NDataSegDetails getSegDetails() {
        return segDetails;
    }

    public void setSegDetails(NDataSegDetails segDetails) {
        checkIsNotCachedAndShared();
        this.segDetails = segDetails;
    }

    public long getLayoutId() {
        return layoutId;
    }

    public void setLayoutId(long layoutId) {
        checkIsNotCachedAndShared();
        this.layoutId = layoutId;
    }

    public String getBuildJobId() {
        return buildJobId;
    }

    public void setBuildJobId(String buildJobId) {
        checkIsNotCachedAndShared();
        this.buildJobId = buildJobId;
    }

    public long getRows() {
        if (CollectionUtils.isEmpty(multiPartition)) {
            return rows;
        }
        return multiPartition.stream().mapToLong(LayoutPartition::getRows).sum();
    }

    public void setRows(long rows) {
        checkIsNotCachedAndShared();
        this.rows = rows;
    }

    public long getByteSize() {
        if (CollectionUtils.isEmpty(multiPartition)) {
            return byteSize;
        }
        return multiPartition.stream().mapToLong(LayoutPartition::getByteSize).sum();
    }

    public void setByteSize(long byteSize) {
        checkIsNotCachedAndShared();
        this.byteSize = byteSize;
    }

    public long getSourceRows() {
        return sourceRows;
    }

    public void setSourceRows(long sourceRows) {
        checkIsNotCachedAndShared();
        this.sourceRows = sourceRows;
    }

    public long getSourceByteSize() {
        return sourceByteSize;
    }

    public void setSourceByteSize(long sourceByteSize) {
        checkIsNotCachedAndShared();
        this.sourceByteSize = sourceByteSize;
    }

    public boolean isEmpty() {
        return rows == 0;
    }

    public boolean isReady() {
        return isReady;
    }

    public void setReady(boolean ready) {
        isReady = ready;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setFileCount(long fileCount) {
        checkIsNotCachedAndShared();
        this.fileCount = fileCount;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        checkIsNotCachedAndShared();
        this.partitionNum = partitionNum;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    public void setPartitionValues(List<String> partitionValues) {
        this.partitionValues = partitionValues;
    }

    public void replacePartitions(List<LayoutPartition> update) {
        HashMap<Long, LayoutPartition> partitionMap = Maps.newHashMap();
        multiPartition.forEach(partition -> partitionMap.put(partition.getPartitionId(), partition));
        update.forEach(partition -> {
            Preconditions.checkState(partitionMap.containsKey(partition.getPartitionId()));
            partitionMap.put(partition.getPartitionId(), partition);
        });
        this.multiPartition = new ArrayList<>(partitionMap.values());
    }

    public boolean removeMultiPartition(Set<Long> toBeDeletedPartIds) {
        val iterator = this.multiPartition.iterator();
        boolean contain = false;
        while (iterator.hasNext()) {
            val dataPartition = iterator.next();
            if (toBeDeletedPartIds.contains(dataPartition.getPartitionId())) {
                iterator.remove();
                contain = true;
            }
        }

        return contain;
    }

    public LayoutPartition getDataPartition(Long partitionId) {
        return this.multiPartition.stream().filter(p -> p.getPartitionId() == partitionId).findAny().orElse(null);
    }

    public List<LayoutPartition> getPartitionsByIds(List<Long> partitionIds) {
        Set<Long> partitionSets = new HashSet<>(partitionIds);
        return multiPartition.stream().filter(partition -> partitionSets.contains(partition.getPartitionId()))
                .collect(Collectors.toList());
    }

    // ============================================================================

    public boolean isCachedAndShared() {
        if (segDetails == null || !segDetails.isCachedAndShared())
            return false;

        for (NDataLayout cached : segDetails.getLayouts()) {
            if (cached == this)
                return true;
        }
        return false;
    }

    public void checkIsNotCachedAndShared() {
        if (isCachedAndShared())
            throw new IllegalStateException();
    }

    public long getIndexId() {
        return (this.getLayoutId() / IndexEntity.INDEX_ID_STEP) * IndexEntity.INDEX_ID_STEP;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Longs.hashCode(layoutId);
        result = prime * result + ((segDetails == null) ? 0 : segDetails.hashCode());
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
        NDataLayout other = (NDataLayout) obj;
        if (layoutId != other.layoutId)
            return false;
        if (segDetails == null) {
            if (other.segDetails != null)
                return false;
        } else if (!segDetails.equals(other.segDetails))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "NDataLayout [ Model Name:" + segDetails.getDataflow().getModelAlias() + ", Segment Id:"
                + segDetails.getId() + ", Layout Id:" + layoutId + "]";
    }
}

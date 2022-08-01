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

package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.kylin.job.common.SegmentUtil;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kyligence.kap.secondstorage.response.SecondStorageNode;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

@Getter
@Setter
public class NDataSegmentResponse extends NDataSegment {

    private static final String SEGMENT_PATH = "segment_path";

    private static final String FILE_COUNT = "file_count";

    @JsonProperty("bytes_size")
    private long bytesSize;

    @JsonProperty("hit_count")
    private long hitCount;

    @JsonProperty("source_bytes_size")
    private long sourceBytesSize;

    @JsonProperty("status_to_display")
    private SegmentStatusEnumToDisplay statusToDisplay;

    @JsonProperty("index_count")
    private long indexCount;

    @JsonProperty("locked_index_count")
    private long lockedIndexCount;

    @JsonProperty("index_count_total")
    private long indexCountTotal;

    @JsonProperty("multi_partition_count")
    private long multiPartitionCount;

    @JsonProperty("multi_partition_count_total")
    private long multiPartitionCountTotal;

    @JsonProperty("source_count")
    private long sourceCount;

    @JsonProperty("row_count")
    private long rowCount;

    @JsonProperty("second_storage_nodes")
    private Map<String, List<SecondStorageNode>> secondStorageNodes;

    // byte
    @JsonProperty("second_storage_size")
    private long secondStorageSize;

    private long createTime;

    private long startTime;

    private long endTime;

    private long storage;

    @JsonProperty("has_base_table_index")
    private boolean hasBaseTableIndex;

    @JsonProperty("has_base_agg_index")
    private boolean hasBaseAggIndex;

    @JsonProperty("has_base_table_index_data")
    private boolean hasBaseTableIndexData;

    public NDataSegmentResponse() {
        super(false);
    }

    public NDataSegmentResponse(NDataflow dataflow, NDataSegment segment) {
        this(dataflow, segment, null);
    }

    public NDataSegmentResponse(NDataflow dataflow, NDataSegment segment, List<AbstractExecutable> executables) {
        super(segment);
        createTime = getCreateTimeUTC();
        startTime = Long.parseLong(getSegRange().getStart().toString());
        endTime = Long.parseLong(getSegRange().getEnd().toString());
        storage = bytesSize;
        indexCount = segment.getLayoutSize();
        indexCountTotal = segment.getIndexPlan().getAllLayoutsSize(true);
        multiPartitionCount = segment.getMultiPartitions().size();
        hasBaseAggIndex = segment.getIndexPlan().containBaseAggLayout();
        hasBaseTableIndex = segment.getIndexPlan().containBaseTableLayout();
        if (segment.getIndexPlan().getBaseTableLayout() != null) {
            val indexPlan = segment.getDataflow().getIndexPlan();
            long segmentFileCount = segment.getSegDetails().getLayouts().stream()
                    .filter(layout -> indexPlan.getLayoutEntity(layout.getLayoutId()).isBaseIndex())
                    .mapToLong(NDataLayout::getFileCount).sum();

            hasBaseTableIndexData = segmentFileCount > 0;
        } else {
            hasBaseTableIndexData = false;
        }
        lockedIndexCount = segment.getIndexPlan().getAllToBeDeleteLayoutId().stream().filter(layoutId ->
                segment.getLayoutsMap().containsKey(layoutId)).count();
        if (dataflow.getModel().getMultiPartitionDesc() != null) {
            multiPartitionCountTotal = dataflow.getModel().getMultiPartitionDesc().getPartitions().size();
        }
        sourceCount = segment.getSourceCount();
        rowCount = segment.getSegDetails().getTotalRowCount();
        setBytesSize(segment.getStorageBytesSize());
        getAdditionalInfo().put(SEGMENT_PATH, dataflow.getSegmentHdfsPath(segment.getId()));
        getAdditionalInfo().put(FILE_COUNT, segment.getStorageFileCount() + "");
        setStatusToDisplay(SegmentUtil.getSegmentStatusToDisplay(dataflow.getSegments(), segment, executables));
        setSourceBytesSize(segment.getSourceBytesSize());
        setLastBuildTime(segment.getLastBuildTime());
        setMaxBucketId(segment.getMaxBucketId());
        lastModifiedTime = getLastBuildTime() != 0 ? getLastBuildTime() : getCreateTime();
    }

    /**
     * for 3x rest api
     */
    @JsonUnwrapped
    @Getter
    @Setter
    private OldParams oldParams;

    @Getter
    @Setter
    public static class OldParams implements Serializable {
        @JsonProperty("size_kb")
        private long sizeKB;

        @JsonProperty("input_records")
        private long inputRecords;
    }

    @JsonProperty("last_modified_time")
    private long lastModifiedTime;

}

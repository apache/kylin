/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.KylinConfigExt;

import java.io.Serializable;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataLayout implements Serializable {
    @JsonBackReference
    private DataSegDetails segDetails;
    @JsonProperty("layout_id")
    private long layoutId;
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
    @JsonProperty("create_time")
    private long createTime;

    public DataLayout() {
        this.createTime = System.currentTimeMillis();
    }

    public KylinConfigExt getConfig() {
        return segDetails.getConfig();
    }

    public static DataLayout newDataLayout(Cube cube, String segId, long layoutId) {
        return newDataLayout(DataSegDetails.newSegDetails(cube, segId), layoutId);
    }

    public static DataLayout newDataLayout(DataSegDetails segDetails, long layoutId) {
        DataLayout r = new DataLayout();
        r.setSegDetails(segDetails);
        r.setLayoutId(layoutId);
        return r;
    }

    public LayoutEntity getLayout() {
        return segDetails.getCube().getSpanningTree().getCuboidLayout(layoutId);
    }

    public DataSegDetails getSegDetails() {
        return segDetails;
    }

    public void setSegDetails(DataSegDetails segDetails) {
        this.segDetails = segDetails;
    }

    public long getLayoutId() {
        return layoutId;
    }

    public void setLayoutId(long layoutId) {
        this.layoutId = layoutId;
    }

    public String getBuildJobId() {
        return buildJobId;
    }

    public void setBuildJobId(String buildJobId) {
        this.buildJobId = buildJobId;
    }

    public long getRows() {
        return rows;
    }

    public void setRows(long rows) {
        this.rows = rows;
    }

    public long getByteSize() {
        return byteSize;
    }

    public void setByteSize(long byteSize) {
        this.byteSize = byteSize;
    }

    public long getFileCount() {
        return fileCount;
    }

    public void setFileCount(long fileCount) {
        this.fileCount = fileCount;
    }

    public long getSourceRows() {
        return sourceRows;
    }

    public void setSourceRows(long sourceRows) {
        this.sourceRows = sourceRows;
    }

    public long getSourceByteSize() {
        return sourceByteSize;
    }

    public void setSourceByteSize(long sourceByteSize) {
        this.sourceByteSize = sourceByteSize;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }
}

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
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Simplify segment description, only preserve start and end timestamp
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataSegment implements Serializable {
    @JsonBackReference
    private Cube cube;

    private String project;

    @JsonProperty("id")
    private String id; // Sequence ID within Cube

    @JsonProperty("name")
    private String name;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("last_build_time")
    private long lastBuildTime; // last segment incr build job

    @JsonProperty("segRange")
    private SegmentRange segmentRange;

    @JsonProperty("source_bytes_size")
    private long sourceBytesSize = -1;

    @JsonProperty("status")
    private SegmentStatusEnum status;

    public DataSegment() { }

    public DataSegment(Cube cube, SegmentRange segmentRange) {
        DataSegment segment = new DataSegment();
        segment.setId(UUID.randomUUID().toString());
        segment.setName(makeSegmentName(segmentRange));
        segment.setCreateTimeUTC(System.currentTimeMillis());
        segment.setCube(cube);
        segment.setSegmentRange(segmentRange);
    }

    private transient DataSegDetails segDetails; // transient, not required by spark cubing
    private transient Map<Long, DataLayout> layoutsMap = Collections.emptyMap(); // transient, not required by spark cubing

    public DataLayout getLayout(long layoutId) {
        return layoutsMap.get(layoutId);
    }

    public DataModel getModel() {
        return this.cube.getModel();
    }

//    public boolean isOffsetCube() {
//        return segmentRange instanceof SegmentRange.KafkaOffsetPartitionedSegmentRange;
//    }
//
    public void setSegmentRange(SegmentRange segmentRange) {
        this.segmentRange = segmentRange;
    }

    public SegmentRange getSegRange() {
        return segmentRange;
    }

    public Cube getCube() {
        return cube;
    }

    public void setCube(Cube cube) {
        this.cube = cube;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCreateTimeUTC() {
        return createTimeUTC;
    }

    public void setCreateTimeUTC(long createTimeUTC) {
        this.createTimeUTC = createTimeUTC;
    }

    public long getLastBuildTime() {
        return lastBuildTime;
    }

    public void setLastBuildTime(long lastBuildTime) {
        this.lastBuildTime = lastBuildTime;
    }

    public Map<Long, DataLayout> getLayoutsMap() {
        return layoutsMap;
    }

    public void setLayoutsMap(Map<Long, DataLayout> layoutsMap) {
        this.layoutsMap = layoutsMap;
    }

    public DataSegDetails getSegDetails() {
        return segDetails;
    }

    public void setSegDetails(DataSegDetails segDetails) {
        this.segDetails = segDetails;
    }

    public long getSourceBytesSize() {
        return sourceBytesSize;
    }

    public void setSourceBytesSize(long sourceBytesSize) {
//        checkIsNotCachedAndShared();
        this.sourceBytesSize = sourceBytesSize;
    }

    public SegmentStatusEnum getStatus() {
        return status;
    }

    public void setStatus(SegmentStatusEnum status) {
        this.status = status;
    }

    public String makeSegmentName(SegmentRange segRange) {
        if (segRange == null || segRange.isInfinite()) {
            return "FULL_BUILD";
        }

        if (segRange instanceof SegmentRange.TimePartitionedSegmentRange) {
            // using time
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.ROOT);
            dateFormat.setTimeZone(TimeZone.getDefault());
            return dateFormat.format(segRange.getStart()) + "_" + dateFormat.format(segRange.getEnd());
        } else {
            return segRange.getStart() + "_" + segRange.getEnd();
        }

    }
}

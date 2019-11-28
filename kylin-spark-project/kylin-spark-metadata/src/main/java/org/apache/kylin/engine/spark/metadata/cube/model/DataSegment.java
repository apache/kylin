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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * Simplify segment description, only preserve start and end timestamp
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataSegment implements Serializable {
    @JsonBackReference
    private Cube cube;

    @JsonProperty("id")
    private String id; // Sequence ID within Cube

    @JsonProperty("name")
    private String name;

    @JsonProperty("create_time_utc")
    private long createTimeUTC;

    @JsonProperty("last_build_time")
    private long lastBuildTime; // last segment incr build job

    @JsonProperty("start")
    private long start;

    @JsonProperty("end")
    private long end;

    private transient DataSegDetails segDetails; // transient, not required by spark cubing
    private transient Map<Long, DataLayout> layoutsMap = Collections.emptyMap(); // transient, not required by spark cubing

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

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public DataSegDetails getSegDetails() {
        return segDetails;
    }

    public void setSegDetails(DataSegDetails segDetails) {
        this.segDetails = segDetails;
    }
}

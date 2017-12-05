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

package org.apache.kylin.storage.druid.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.timeline.DataSegment;

import java.util.Objects;
import java.util.Set;

/**
 * copy from druid
 */
public class SegmentPublishResult {
    private final Set<DataSegment> segments;
    private final boolean success;

    public SegmentPublishResult(Set<DataSegment> segments, boolean success) {
        this.segments = Preconditions.checkNotNull(segments, "segments");
        this.success = success;

        if (!success) {
            Preconditions.checkArgument(segments.isEmpty(), "segments must be empty for unsuccessful publishes");
        }
    }

    @JsonProperty
    public Set<DataSegment> getSegments() {
        return segments;
    }

    @JsonProperty
    public boolean isSuccess() {
        return success;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SegmentPublishResult that = (SegmentPublishResult) o;
        return success == that.success && Objects.equals(segments, that.segments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segments, success);
    }

    @Override
    public String toString() {
        return "SegmentPublishResult{" + "segments=" + segments + ", success=" + success + '}';
    }
}

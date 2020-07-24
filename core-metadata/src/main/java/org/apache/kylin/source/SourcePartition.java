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

package org.apache.kylin.source;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;

import org.apache.kylin.shaded.com.google.common.base.MoreObjects;

/**
 * Defines a set of source records that will be built into a cube segment.
 * 
 * There are two main approaches:
 * 1) by a date range, in case of time partitioned tables like Hive.
 * 2) by an offset range, in case of offset based source like Kafka.
 * 
 * For the offset approach, the source can further be partitioned and each partition can define
 * its own start and end offset within that partition.
 */
public class SourcePartition {
    TSRange tsRange;
    SegmentRange segRange;
    Map<Integer, Long> sourcePartitionOffsetStart;
    Map<Integer, Long> sourcePartitionOffsetEnd;

    public SourcePartition() {
    }

    public SourcePartition(TSRange tsRange, SegmentRange segRange, Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd) {
        this.tsRange = tsRange;
        this.segRange = segRange;
        this.sourcePartitionOffsetStart = sourcePartitionOffsetStart;
        this.sourcePartitionOffsetEnd = sourcePartitionOffsetEnd;
    }

    public TSRange getTSRange() {
        return tsRange;
    }

    public void setTSRange(TSRange tsRange) {
        this.tsRange = tsRange;
    }

    public SegmentRange getSegRange() {
        return segRange;
    }

    public void setSegRange(SegmentRange segRange) {
        this.segRange = segRange;
    }

    public Map<Integer, Long> getSourcePartitionOffsetStart() {
        return sourcePartitionOffsetStart;
    }

    public void setSourcePartitionOffsetStart(Map<Integer, Long> sourcePartitionOffsetStart) {
        this.sourcePartitionOffsetStart = sourcePartitionOffsetStart;
    }

    public Map<Integer, Long> getSourcePartitionOffsetEnd() {
        return sourcePartitionOffsetEnd;
    }

    public void setSourcePartitionOffsetEnd(Map<Integer, Long> sourcePartitionOffsetEnd) {
        this.sourcePartitionOffsetEnd = sourcePartitionOffsetEnd;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("tsRange", tsRange).add("segRange", segRange).add("sourcePartitionOffsetStart", sourcePartitionOffsetStart.toString()).add("sourcePartitionOffsetEnd", sourcePartitionOffsetEnd.toString()).toString();
    }

    public static SourcePartition getCopyOf(SourcePartition origin) {
        SourcePartition copy = new SourcePartition();
        copy.setTSRange(origin.getTSRange());
        copy.setSegRange(origin.getSegRange());
        if (origin.getSourcePartitionOffsetStart() != null) {
            copy.setSourcePartitionOffsetStart(new HashMap<>(origin.getSourcePartitionOffsetStart()));
        }
        if (origin.getSourcePartitionOffsetEnd() != null) {
            copy.setSourcePartitionOffsetEnd(new HashMap<>(origin.getSourcePartitionOffsetEnd()));
        }
        return copy;
    }
}

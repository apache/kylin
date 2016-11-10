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

import com.google.common.base.Objects;

/**
 */
public class SourcePartition {
    long startDate;
    long endDate;
    long startOffset;
    long endOffset;
    Map<Integer, Long> sourcePartitionOffsetStart;
    Map<Integer, Long> sourcePartitionOffsetEnd;

    public SourcePartition() {
    }

    public SourcePartition(long startDate, long endDate, long startOffset, long endOffset, Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.sourcePartitionOffsetStart = sourcePartitionOffsetStart;
        this.sourcePartitionOffsetEnd = sourcePartitionOffsetEnd;
    }

    public long getStartDate() {
        return startDate;
    }

    public void setStartDate(long startDate) {
        this.startDate = startDate;
    }

    public long getEndDate() {
        return endDate;
    }

    public void setEndDate(long endDate) {
        this.endDate = endDate;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset = endOffset;
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
        return Objects.toStringHelper(this).add("startDate", startDate).add("endDate", endDate).add("startOffset", startOffset).add("endOffset", endOffset).add("sourcePartitionOffsetStart", sourcePartitionOffsetStart.toString()).add("sourcePartitionOffsetEnd", sourcePartitionOffsetEnd.toString()).toString();
    }

    public static SourcePartition getCopyOf(SourcePartition origin) {
        SourcePartition copy = new SourcePartition();
        copy.setStartDate(origin.getStartDate());
        copy.setEndDate(origin.getEndDate());
        copy.setStartOffset(origin.getStartOffset());
        copy.setEndOffset(origin.getEndOffset());
        if (origin.getSourcePartitionOffsetStart() != null) {
            copy.setSourcePartitionOffsetStart(new HashMap<>(origin.getSourcePartitionOffsetStart()));
        }
        if (origin.getSourcePartitionOffsetEnd() != null) {
            copy.setSourcePartitionOffsetEnd(new HashMap<>(origin.getSourcePartitionOffsetEnd()));
        }
        return copy;
    }
}

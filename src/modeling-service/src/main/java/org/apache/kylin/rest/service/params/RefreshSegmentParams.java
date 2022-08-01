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
package org.apache.kylin.rest.service.params;

import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class RefreshSegmentParams extends BasicSegmentParams {
    private String[] segmentIds;
    private boolean refreshAllLayouts;
    private Set<Long> partitions;

    public RefreshSegmentParams(String project, String modelId, String[] segmentIds) {
        super(project, modelId);
        this.segmentIds = segmentIds;
    }

    public RefreshSegmentParams(String project, String modelId, String[] segmentIds, boolean refreshAllLayouts) {
        this(project, modelId, segmentIds, refreshAllLayouts, null);
    }

    public RefreshSegmentParams(String project, String modelId, String[] segmentIds, boolean refreshAllLayouts,
            Set<Long> partitions) {
        this(project, modelId, segmentIds);
        this.refreshAllLayouts = refreshAllLayouts;
        this.partitions = partitions;
    }

    public RefreshSegmentParams withIgnoredSnapshotTables(Set<String> ignoredSnapshotTables) {
        this.ignoredSnapshotTables = ignoredSnapshotTables;
        return this;
    }

    public RefreshSegmentParams withPriority(int priority) {
        this.priority = priority;
        return this;
    }

    public RefreshSegmentParams withPartialBuild(boolean partialBuild) {
        this.partialBuild = partialBuild;
        return this;
    }

    public RefreshSegmentParams withBatchIndexIds(List<Long> batchIndexIds) {
        this.batchIndexIds = batchIndexIds;
        return this;
    }

    public RefreshSegmentParams withYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
        return this;
    }

    public RefreshSegmentParams withTag(Object tag) {
        this.tag = tag;
        return this;
    }
}

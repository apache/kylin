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
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.MultiPartitionDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.rest.request.SegmentTimeRequest;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
public class IncrementBuildSegmentParams extends FullBuildSegmentParams {
    private String start;
    private String end;
    private PartitionDesc partitionDesc;
    private MultiPartitionDesc multiPartitionDesc;
    private List<SegmentTimeRequest> segmentHoles;
    private String partitionColFormat;
    private List<String[]> multiPartitionValues;
    private boolean buildAllSubPartitions;

    public IncrementBuildSegmentParams(String project, String modelId, String start, String end,
            String partitionColFormat, boolean needBuild, List<String[]> multiPartitionValues) {
        super(project, modelId, needBuild);
        this.start = start;
        this.end = end;
        this.partitionColFormat = partitionColFormat;
        this.multiPartitionValues = multiPartitionValues;
    }

    public IncrementBuildSegmentParams(String project, String modelId, String start, String end,
            PartitionDesc partitionDesc, MultiPartitionDesc multiPartitionDesc, List<SegmentTimeRequest> segmentHoles,
            boolean needBuild, List<String[]> multiPartitionValues) {
        super(project, modelId, needBuild);
        this.start = start;
        this.end = end;
        this.partitionDesc = partitionDesc;
        this.segmentHoles = segmentHoles;
        this.multiPartitionValues = multiPartitionValues;
        this.multiPartitionDesc = multiPartitionDesc;
    }

    public IncrementBuildSegmentParams(String project, String modelId, String start, String end,
            PartitionDesc partitionDesc, MultiPartitionDesc multiPartitionDesc, String partitionColFormat,
            List<SegmentTimeRequest> segmentHoles, boolean needBuild, List<String[]> multiPartitionValues) {
        super(project, modelId, needBuild);
        this.start = start;
        this.end = end;
        this.partitionDesc = partitionDesc;
        this.segmentHoles = segmentHoles;
        this.partitionColFormat = partitionColFormat;
        this.multiPartitionValues = multiPartitionValues;
        this.multiPartitionDesc = multiPartitionDesc;
    }

    @Override
    public IncrementBuildSegmentParams withIgnoredSnapshotTables(Set<String> ignoredSnapshotTables) {
        this.ignoredSnapshotTables = ignoredSnapshotTables;
        return this;
    }

    @Override
    public IncrementBuildSegmentParams withPriority(int priority) {
        this.priority = priority;
        return this;
    }

    @Override
    public IncrementBuildSegmentParams withPartialBuild(boolean partialBuild) {
        this.partialBuild = partialBuild;
        return this;
    }

    @Override
    public IncrementBuildSegmentParams withBatchIndexIds(List<Long> batchIndexIds) {
        this.batchIndexIds = batchIndexIds;
        return this;
    }

    @Override
    public IncrementBuildSegmentParams withYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
        return this;
    }

    @Override
    public IncrementBuildSegmentParams withTag(Object tag) {
        this.tag = tag;
        return this;
    }

    public IncrementBuildSegmentParams withBuildAllSubPartitions(boolean buildAllSubPartitions) {
        this.buildAllSubPartitions = buildAllSubPartitions;
        return this;
    }

    public List<String[]> getMultiPartitionValues() {
        List<String[]> mixedMultiPartitionValues = this.multiPartitionValues;
        if (this.buildAllSubPartitions) {
            NDataModel model = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .getDataModelDesc(modelId);
            MultiPartitionDesc modelMultiPartitionDesc = model.getMultiPartitionDesc();
            List<String[]> allPartitionValues = Lists.newArrayList();
            if (modelMultiPartitionDesc != null) { // in case model multiPartitionDesc has been changed to null
                allPartitionValues = modelMultiPartitionDesc.getPartitions().stream()
                        .map(MultiPartitionDesc.PartitionInfo::getValues).collect(Collectors.toList());
            }
            if (mixedMultiPartitionValues != null) {
                mixedMultiPartitionValues.addAll(allPartitionValues);
            } else {
                mixedMultiPartitionValues = allPartitionValues;
            }
        }
        return mixedMultiPartitionValues;
    }
}

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

package org.apache.kylin.engine.spark.model;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.cuboid.AdaptiveSpanningTree;
import org.apache.kylin.metadata.cube.model.NDataSegment;

import com.google.common.base.Preconditions;

public class PartitionFlatTableDesc extends SegmentFlatTableDesc {

    private final String jobId;
    private final List<Long> partitions;

    public PartitionFlatTableDesc(KylinConfig config, NDataSegment dataSegment, AdaptiveSpanningTree spanningTree, //
            String jobId, List<Long> partitions) {
        super(config, dataSegment, spanningTree);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(partitions);
        this.jobId = jobId;
        this.partitions = Collections.unmodifiableList(partitions);
    }

    public PartitionFlatTableDesc(KylinConfig config, NDataSegment dataSegment, AdaptiveSpanningTree spanningTree, //
            List<String> relatedTables, String jobId, List<Long> partitions) {
        super(config, dataSegment, spanningTree, relatedTables);
        Preconditions.checkNotNull(jobId);
        Preconditions.checkNotNull(partitions);
        this.jobId = jobId;
        this.partitions = Collections.unmodifiableList(partitions);
    }

    public List<Long> getPartitions() {
        return this.partitions;
    }

    @Override
    public Path getFlatTablePath() {
        return config.getJobTmpFlatTableDir(project, jobId);
    }

    @Override
    public Path getFactTableViewPath() {
        return config.getJobTmpFactTableViewDir(project, jobId);
    }

    @Override
    public boolean shouldPersistFlatTable() {
        return true;
    }

    @Override
    protected void initColumns() {
        // Ensure the partition columns were included.
        addPartitionColumns();
        // By design, only indexPlan columns would be included.
        if (isPartialBuild()) {
            addIndexPartialBuildColumns();
        } else {
            addIndexPlanColumns();
        }
    }

    private void addPartitionColumns() {
        dataModel.getMultiPartitionDesc() //
                .getColumnRefs() //
                .stream().filter(Objects::nonNull) //
                .forEach(this::addColumn);
    }
}

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

package org.apache.kylin.streaming.jobs;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.engine.spark.job.BuildJobInfos;
import org.apache.kylin.engine.spark.job.DFMergeJob;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.streaming.common.MergeJobEntry;
import org.apache.kylin.streaming.metadata.BuildLayoutWithRestUpdate;

import lombok.val;

public class StreamingDFMergeJob extends DFMergeJob {

    private MergeJobEntry mergeJobEntry;

    public StreamingDFMergeJob() {
        buildLayoutWithUpdate = new BuildLayoutWithRestUpdate(JobTypeEnum.STREAMING_MERGE);
        this.config = KylinConfig.getInstanceFromEnv();
    }

    public void streamingMergeSegment(MergeJobEntry mergeJobEntry) throws IOException {
        this.mergeJobEntry = mergeJobEntry;
        this.ss = mergeJobEntry.spark();
        this.project = mergeJobEntry.project();
        ss.sparkContext().setLocalProperty("spark.sql.execution.id", null);
        val specifiedCuboids = mergeJobEntry.afterMergeSegment().getLayoutsMap().keySet();
        this.jobId = RandomUtil.randomUUIDStr();
        if (this.infos == null) {
            this.infos = new BuildJobInfos();
        } else {
            this.infos.clear();
        }

        this.mergeSegments(mergeJobEntry.dataflowId(), mergeJobEntry.afterMergeSegment().getId(), specifiedCuboids);
    }

    public void shutdown() {
        buildLayoutWithUpdate.shutDown();
    }

    @Override
    protected List<NDataSegment> getMergingSegments(NDataflow dataflow, NDataSegment mergedSeg) {
        return mergeJobEntry.unMergedSegments();
    }
}

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

package org.apache.kylin.engine.spark.job;

import static org.apache.kylin.engine.spark.job.StageType.MERGE_COLUMN_BYTES;
import static org.apache.kylin.engine.spark.job.StageType.MERGE_FLAT_TABLE;
import static org.apache.kylin.engine.spark.job.StageType.MERGE_INDICES;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.job.exec.MergeExec;
import org.apache.kylin.engine.spark.job.stage.BuildParam;
import org.apache.kylin.metadata.cube.model.NDataSegment;

import com.google.common.base.Throwables;

import lombok.val;

public class SegmentMergeJob extends SegmentJob {

    public static void main(String[] args) {
        SegmentMergeJob segmentMergeJob = new SegmentMergeJob();
        segmentMergeJob.execute(args);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

    @Override
    protected final void doExecute() throws Exception {
        merge();
    }

    private void merge() throws IOException {
        Stream<NDataSegment> segmentStream = config.isSegmentParallelBuildEnabled() ? //
                readOnlySegments.parallelStream() : readOnlySegments.stream();
        AtomicLong finishedSegmentCount = new AtomicLong(0);
        val segmentsCount = readOnlySegments.size();
        segmentStream.forEach(seg -> {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
                val exec = new MergeExec(jobStepId);

                val buildParam = new BuildParam();
                MERGE_FLAT_TABLE.createStage(this, seg, buildParam, exec);
                MERGE_INDICES.createStage(this, seg, buildParam, exec);

                exec.mergeSegment();

                val mergeColumnBytes = MERGE_COLUMN_BYTES.createStage(this, seg, buildParam, exec);
                mergeColumnBytes.toWorkWithoutFinally();

                if (finishedSegmentCount.incrementAndGet() < segmentsCount) {
                    mergeColumnBytes.onStageFinished(true);
                }
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        });
    }
}

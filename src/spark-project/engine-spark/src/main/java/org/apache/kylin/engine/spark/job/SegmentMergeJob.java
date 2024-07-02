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

import static org.apache.kylin.engine.spark.job.StageEnum.MERGE_COLUMN_BYTES;
import static org.apache.kylin.engine.spark.job.StageEnum.MERGE_FLAT_TABLE;
import static org.apache.kylin.engine.spark.job.StageEnum.MERGE_INDICES;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.spark.tracker.AppStatusContext;

import lombok.val;

public class SegmentMergeJob extends SegmentJob {

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.dfMergeJobInfo();
    }

    @Override
    protected final void doExecute() throws Exception {

        appStatusContext = new AppStatusContext(getSparkSession().sparkContext(), config);
        appStatusContext.appStatusTracker().startMonitorBuildResourceState();

        merge();
    }

    private void merge() throws IOException {
        Stream<NDataSegment> segmentStream = //
                config.isSegmentParallelBuildEnabled() //
                        ? readOnlySegments.parallelStream() //
                        : readOnlySegments.stream();
        AtomicLong finishedSegmentCount = new AtomicLong(0);
        val segmentsCount = readOnlySegments.size();
        segmentStream.forEach(seg -> {
            try (KylinConfig.SetAndUnsetThreadLocalConfig autoCloseConfig = KylinConfig
                    .setAndUnsetThreadLocalConfig(config)) {
                val stepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
                val step = new StepExec(stepId);

                step.addStage(MERGE_FLAT_TABLE.createExec(this, seg));
                step.addStage(MERGE_INDICES.createExec(this, seg));

                step.doExecute();

                val mergeColumnBytes = MERGE_COLUMN_BYTES.createExec(this, seg);
                step.addStage(mergeColumnBytes);
                mergeColumnBytes.doExecuteWithoutFinally();

                if (finishedSegmentCount.incrementAndGet() < segmentsCount) {
                    mergeColumnBytes.onStageFinished(ExecutableState.SUCCEED);
                }
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        });
    }

    public static void main(String[] args) {
        SegmentMergeJob segmentMergeJob = new SegmentMergeJob();
        segmentMergeJob.execute(args);
    }
}

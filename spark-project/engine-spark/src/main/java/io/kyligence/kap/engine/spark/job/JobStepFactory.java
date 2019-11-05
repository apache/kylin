/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.job;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.metadata.model.Segments;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;

public class JobStepFactory {

    private JobStepFactory() {
    }

    public static NSparkExecutable addStep(DefaultChainedExecutable parent, JobStepType type) {
        NSparkExecutable step;
        if (type == JobStepType.RESOURCE_DETECT) {
            step = new NResourceDetectStep(parent);
        } else {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            step = new NTableSamplingJob.SamplingStep(config.getSparkTableSamplingClassName());
        }

        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setJobType(parent.getJobType());
        parent.addTask(step);
        //after addTask, step's id is changed

        step.setDistMetaUrl(
                KylinConfig.getInstanceFromEnv().getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        return step;
    }

    public static NSparkExecutable addStep(DefaultChainedExecutable parent, JobStepType type,
            Set<NDataSegment> segments) {
        NSparkExecutable step;
        NDataflow df = segments.iterator().next().getDataflow();
        KylinConfigExt config = df.getConfig();
        switch (type) {
        case RESOURCE_DETECT:
            step = new NResourceDetectStep(parent);
            break;
        case CUBING:
            step = new NSparkCubingStep(config.getSparkBuildClassName());
            break;
        case MERGING:
            step = new NSparkMergingStep(config.getSparkMergeClassName());
            break;
        case CLEAN_UP_AFTER_MERGE:
            step = new NSparkCleanupAfterMergeStep();
            break;
        default:
            throw new IllegalArgumentException();
        }

        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        step.setJobType(parent.getJobType());
        if (step instanceof NSparkCleanupAfterMergeStep) {
            final Segments<NDataSegment> mergingSegments = df.getMergingSegments(segments.iterator().next());
            step.setParam(NBatchConstants.P_SEGMENT_IDS,
                    String.join(",", NSparkCubingUtil.toSegmentIds(mergingSegments)));
        }
        parent.addTask(step);
        //after addTask, step's id is changed
        step.setDistMetaUrl(config.getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        return step;
    }
}

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

import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NResourceDetectStep extends NSparkExecutable {

    // called by reflection
    public NResourceDetectStep() {

    }

    public NResourceDetectStep(DefaultChainedExecutable parent) {
        if (parent instanceof NSparkCubingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeCubingJob.class.getName());
        } else if (parent instanceof NSparkMergingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeMergingJob.class.getName());
        } else if (parent instanceof NTableSamplingJob) {
            this.setSparkSubmitClassName(ResourceDetectBeforeSampling.class.getName());
        } else {
            throw new IllegalArgumentException("Unsupported resource detect for " + parent.getName() + " job");
        }
        this.setName(ExecutableConstants.STEP_NAME_DETECT_RESOURCE);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        final AbstractExecutable parent = getParent();
        if (parent instanceof DefaultChainedExecutable) {
            return ((DefaultChainedExecutable) parent).getMetadataDumpList(config);
        }
        throw new IllegalStateException("Unsupported resource detect for non chained executable!");
    }

    @Override
    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> sparkConfigOverride = super.getSparkConfigOverride(config);
        sparkConfigOverride.put("spark.master", "local");
        sparkConfigOverride.put("spark.sql.autoBroadcastJoinThreshold", "-1");
        sparkConfigOverride.put("spark.sql.adaptive.enabled", "false");
        return sparkConfigOverride;
    }
}

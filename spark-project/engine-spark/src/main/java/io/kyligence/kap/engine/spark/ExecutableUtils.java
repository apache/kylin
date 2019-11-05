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
package io.kyligence.kap.engine.spark;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.job.execution.AbstractExecutable;

import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import lombok.val;

public class ExecutableUtils {

    public static ResourceStore getRemoteStore(KylinConfig config, AbstractExecutable buildTask) {
        val buildStepUrl = buildTask.getParam(NBatchConstants.P_OUTPUT_META_URL);

        val buildConfig = KylinConfig.createKylinConfig(config);
        buildConfig.setMetadataUrl(buildStepUrl);
        return ResourceStore.getKylinMetaStore(buildConfig);
    }

    public static String getDataflowId(AbstractExecutable buildTask) {
        return buildTask.getParam(NBatchConstants.P_DATAFLOW_ID);
    }

    public static Set<String> getSegmentIds(AbstractExecutable buildTask) {
        return Stream.of(StringUtils.split(buildTask.getParam(NBatchConstants.P_SEGMENT_IDS), ","))
                .collect(Collectors.toSet());
    }

    public static Set<Long> getLayoutIds(AbstractExecutable buildTask) {
        return Stream.of(StringUtils.split(buildTask.getParam(NBatchConstants.P_LAYOUT_IDS), ","))
                .map(Long::parseLong).collect(Collectors.toSet());

    }
}

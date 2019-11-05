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

import java.util.Map;
import java.util.Set;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;

import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentUtils {

    public static Set<LayoutEntity> getToBuildLayouts(NDataflow df) {
        Set<LayoutEntity> layouts = Sets.newHashSet();
        Segments<NDataSegment> readySegments = df.getSegments(SegmentStatusEnum.READY);

        if (CollectionUtils.isEmpty(readySegments)) {
            if (CollectionUtils.isNotEmpty(df.getIndexPlan().getAllIndexes())) {
                layouts.addAll(df.getIndexPlan().getAllLayouts());
            }
            log.trace("added {} layouts according to model {}'s index plan", layouts.size(),
                    df.getIndexPlan().getModel().getAlias());
        } else {
            NDataSegment latestReadySegment = readySegments.getLatestReadySegment();
            for (Map.Entry<Long, NDataLayout> cuboid : latestReadySegment.getLayoutsMap().entrySet()) {
                layouts.add(cuboid.getValue().getLayout());
            }
            log.trace("added {} layouts according to model {}'s latest ready segment {}", layouts.size(),
                    df.getIndexPlan().getModel().getAlias(), latestReadySegment.getName());
        }
        return layouts;
    }
}

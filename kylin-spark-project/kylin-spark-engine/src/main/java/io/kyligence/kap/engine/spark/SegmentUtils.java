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

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

package org.apache.kylin.metadata.cube.optimization;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.utils.IndexPlanReduceUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MergedLayoutOptStrategy extends AbstractOptStrategy {

    public MergedLayoutOptStrategy() {
        this.setType(GarbageLayoutType.MERGED);
    }

    @Override
    protected Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog) {
        Set<Long> garbageLayouts = Sets.newHashSet();
        List<Set<LayoutEntity>> sameDimAggLayouts = IndexPlanReduceUtil.collectSameDimAggLayouts(inputLayouts);
        garbageLayouts.addAll(
                sameDimAggLayouts.stream().flatMap(Set::stream).map(LayoutEntity::getId).collect(Collectors.toSet()));

        if (needLog) {
            log.info("In dataflow({}), MergeLayoutOptStrategy found garbage laoyouts: {}.", dataflow.getId(),
                    garbageLayouts);
        }
        return garbageLayouts;
    }

    @Override
    protected void skipOptimizeIndex(List<LayoutEntity> inputLayouts) {
        inputLayouts.removeIf(layout -> layout.isBase() || IndexEntity.isTableIndex(layout.getId()));
    }
}

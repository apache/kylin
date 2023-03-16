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

import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflow;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public abstract class AbstractOptStrategy {

    @Getter
    @Setter(AccessLevel.PROTECTED)
    private GarbageLayoutType type;

    /**
     * Every subclass of AbstractStrategy should override this method.
     */
    protected abstract Set<Long> doCollect(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog);

    public final Set<Long> collectGarbageLayouts(List<LayoutEntity> inputLayouts, NDataflow dataflow, boolean needLog) {
        List<LayoutEntity> toHandleLayouts = beforeCollect(inputLayouts);
        Set<Long> garbageSet = doCollect(toHandleLayouts, dataflow, needLog);
        afterCollect(inputLayouts, garbageSet);
        return garbageSet;
    }

    private List<LayoutEntity> beforeCollect(List<LayoutEntity> inputLayouts) {
        List<LayoutEntity> layoutsToHandle = Lists.newArrayList(inputLayouts);
        skipOptimizeTableIndex(layoutsToHandle);
        return layoutsToHandle;
    }

    protected abstract void skipOptimizeTableIndex(List<LayoutEntity> inputLayouts);

    private void afterCollect(List<LayoutEntity> inputLayouts, Set<Long> garbages) {
        inputLayouts.removeIf(layout -> garbages.contains(layout.getId()));
    }
}

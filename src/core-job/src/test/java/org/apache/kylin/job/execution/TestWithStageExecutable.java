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

package org.apache.kylin.job.execution;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import lombok.Getter;
import lombok.Setter;

/**
 */
public class TestWithStageExecutable extends BaseTestExecutable implements ChainedStageExecutable {

    private transient final List<StageBase> stages = Lists.newCopyOnWriteArrayList();
    private final Map<String, List<StageBase>> stagesMap = Maps.newConcurrentMap();

    @Setter
    @Getter
    private Set<String> segmentIds = Sets.newHashSet();

    public TestWithStageExecutable() {
        super();
        this.setName("SucceedDagTestExecutable");
    }

    public TestWithStageExecutable(Object notSetId) {
        super(notSetId);
    }

    @Override
    public ExecuteResult doWork(ExecutableContext context) {
        return ExecuteResult.createSucceed();
    }

    @Override
    protected boolean needCheckState() {
        return false;
    }

    @Override
    public Map<String, List<StageBase>> getStagesMap() {
        return stagesMap;
    }

    @Override
    public void setStageMap() {
        if (CollectionUtils.isEmpty(stages)) {
            return;
        }
        // when table sampling and snapshot build, null segmentIds, use jobId
        if (CollectionUtils.isEmpty(segmentIds)) {
            stagesMap.put(getId(), stages);
            return;
        }
        for (String segmentId : segmentIds) {
            stagesMap.put(segmentId, stages);
        }
    }

    @Override
    public void setStageMapWithSegment(String id, List<StageBase> steps) {
        final List<StageBase> old = stagesMap.getOrDefault(id, Lists.newCopyOnWriteArrayList());
        old.addAll(steps);
        stagesMap.put(id, steps);
    }

    @Override
    public AbstractExecutable addStage(AbstractExecutable step) {
        int stepId = stages.size();

        step.setId(getId() + "_" + String.format(Locale.ROOT, "%02d", stepId));
        step.setParent(this);
        step.setStepId(stepId);
        this.stages.add(((StageBase) step));
        return step;
    }
}

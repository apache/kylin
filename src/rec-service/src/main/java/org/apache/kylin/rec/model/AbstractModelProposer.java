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

package org.apache.kylin.rec.model;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.AbstractContext;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractModelProposer {

    protected AbstractContext.ModelContext modelContext;
    final String project;
    final NDataModelManager dataModelManager;

    AbstractModelProposer(AbstractContext.ModelContext modelCtx) {
        this.modelContext = modelCtx;

        project = modelCtx.getProposeContext().getProject();

        dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
    }

    public AbstractContext.ModelContext getModelContext() {
        return modelContext;
    }

    public NDataModel propose(NDataModel origModel) {
        NDataModel modelDesc;
        try {
            modelDesc = JsonUtil.deepCopy(origModel, NDataModel.class);
            modelDesc.setComputedColumnDescs(ComputedColumnUtil.deepCopy(origModel.getComputedColumnDescs()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deep copy model", e);
        }

        initModel(modelDesc);
        execute(modelDesc);
        initModel(modelDesc);
        return modelDesc;
    }

    void initModel(NDataModel modelDesc) {
        modelDesc.init(KylinConfig.getInstanceFromEnv(), project, Lists.newArrayList());
    }

    boolean isValidOlapContext(OlapContext context) {
        val accelerateInfo = modelContext.getProposeContext().getAccelerateInfoMap().get(context.getSql());
        return accelerateInfo != null && !accelerateInfo.isNotSucceed();
    }

    protected abstract void execute(NDataModel modelDesc);
}

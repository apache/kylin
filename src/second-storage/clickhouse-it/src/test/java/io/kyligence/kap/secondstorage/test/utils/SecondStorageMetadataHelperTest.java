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

package io.kyligence.kap.secondstorage.test.utils;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.NExecutableManager;

import java.util.List;

public abstract class SecondStorageMetadataHelperTest {

    public abstract String getProject();

    public abstract String getModelId();

    protected KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    protected TableFlow getTableFlow() {
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(getModelId()).isPresent());
        return SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(getModelId()).get();
    }

    protected TablePlan getTablePlan() {
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(getModelId()).isPresent());
        return SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(getModelId()).get();
    }

    protected IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(getConfig(), getProject()).getIndexPlan(getModelId());
    }

    protected NDataflow getDataFlow() {
        return NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getDataflow(getModelId());
    }

    protected NDataModelManager getNDataModelManager() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    protected NDataModel getNDataModel() {
        return getNDataModelManager().getDataModelDesc(getModelId());
    }

    protected NExecutableManager getNExecutableManager() {
        return NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    protected List<NodeGroup> getNodeGroups() {
        Preconditions.checkState(SecondStorageUtil.nodeGroupManager(getConfig(), getProject()).isPresent());
        return SecondStorageUtil.nodeGroupManager(getConfig(), getProject()).get().listAll();
    }
}

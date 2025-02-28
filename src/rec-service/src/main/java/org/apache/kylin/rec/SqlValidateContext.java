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

package org.apache.kylin.rec;

import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.model.AbstractJoinRule;

public class SqlValidateContext extends AbstractSemiContext {

    public SqlValidateContext(KylinConfig kylinConfig, String project, String[] sqls) {
        super(kylinConfig, project, sqls);
        this.canCreateNewModel = getSmartConfig().getModelOptRule().equalsIgnoreCase(AbstractJoinRule.APPEND);
        this.partialMatch = kylinConfig.isQueryMatchPartialInnerJoinModel();
    }

    @Override
    public ChainedProposer createProposers() {
        return new ChainedProposer(this, ImmutableList.of());
    }

    @Override
    public IndexPlan getOriginIndexPlan(String modelId) {
        throw new NotImplementedException("Fetch origin indexes is forbidden in SqlValidateContext!");
    }

    @Override
    public List<NDataModel> getOriginModels() {
        throw new NotImplementedException("Fetch origin models is forbidden in SqlValidateContext!");

    }

    @Override
    public void changeModelMainType(NDataModel model) {
        throw new NotImplementedException("Modifying ModelMaintainType is forbidden in SqlValidateContext");
    }
}

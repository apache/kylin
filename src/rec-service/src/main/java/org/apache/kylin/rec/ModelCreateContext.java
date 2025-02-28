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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NDataModel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ModelCreateContext extends AbstractSemiContext {

    public ModelCreateContext(KylinConfig kylinConfig, String project, String[] sqlArray) {
        this(kylinConfig, project, sqlArray, "");
    }

    public ModelCreateContext(KylinConfig kylinConfig, String project, String[] sqlArray, String modelName) {
        super(kylinConfig, project, sqlArray, modelName);
        this.canCreateNewModel = true;
    }

    @Override
    public List<NDataModel> getOriginModels() {
        return Lists.newArrayList();
    }

    @Override
    public ChainedProposer createProposers() {
        ImmutableList<AbstractProposer> proposers = ImmutableList.of(//
                new SQLAnalysisProposer(this), //
                new ModelOptProposer(this), //
                new ModelInfoAdjustProposer(this), //
                new ModelRenameProposer(this), //
                new IndexPlanSelectProposer(this), //
                new IndexPlanOptProposer(this), //
                new IndexPlanShrinkProposer(this) //
        );
        return new ChainedProposer(this, proposers);
    }
}

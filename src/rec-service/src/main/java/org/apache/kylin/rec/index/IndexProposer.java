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

package org.apache.kylin.rec.index;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexEntity.IndexIdentifier;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rec.AbstractContext;

class IndexProposer extends AbstractIndexProposer {

    IndexProposer(AbstractContext.ModelContext context) {
        super(context);
    }

    @Override
    public IndexPlan execute(IndexPlan indexPlan) {

        Map<IndexIdentifier, IndexEntity> indexEntityMap = indexPlan.getAllIndexesMap();

        IndexSuggester suggester = new IndexSuggester(context, indexPlan, indexEntityMap);
        suggester.suggestIndexes(context.getModelTree());

        indexPlan.setIndexes(Lists.newArrayList(indexEntityMap.values()));
        initModel(context.getTargetModel());
        return indexPlan;
    }

    // Important fix for KE-14714: all columns in table index will be treat as dimension.
    // Extra dimensions may add to model in IndexSuggester.suggestTableIndexDimensions,
    // code line: namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION).
    private void initModel(NDataModel modelDesc) {
        String project = context.getProposeContext().getProject();
        modelDesc.init(KylinConfig.getInstanceFromEnv(), project, Lists.newArrayList());
    }
}

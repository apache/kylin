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

import java.util.List;
import java.util.Map;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.rec.AbstractContext;

public class ShrinkComputedColumnProposer extends AbstractModelProposer {

    public ShrinkComputedColumnProposer(AbstractContext.ModelContext modelCtx) {
        super(modelCtx);
    }

    @Override
    protected void execute(NDataModel modelDesc) {
        discardRedundantRecommendations(modelDesc);
        discardCCFieldIfNotApply(modelDesc);
    }

    private void discardRedundantRecommendations(NDataModel dataModel) {
        if (modelContext.getProposeContext().skipCollectRecommendations()) {
            return;
        }
        Map<String, CCRecItemV2> recItemMap = modelContext.getCcRecItemMap();
        dataModel.getComputedColumnDescs().forEach(cc -> {
            if (!isCCUsedByModel(cc, dataModel)) {
                recItemMap.remove(cc.getUuid());
            }
        });
    }

    private void discardCCFieldIfNotApply(NDataModel model) {
        List<ComputedColumnDesc> originCC = Lists.newArrayList(model.getComputedColumnDescs());
        originCC.removeIf(computedColumnDesc -> !isCCUsedByModel(computedColumnDesc, model));
        model.setComputedColumnDescs(originCC);
    }

    private boolean isCCUsedByModel(ComputedColumnDesc cc, NDataModel model) {
        // check dimension
        for (NDataModel.NamedColumn column : model.getAllNamedColumns()) {
            if (column.isExist() && column.getAliasDotColumn().equals(cc.getFullName())) {
                return true;
            }
        }

        // check measure
        for (NDataModel.Measure allMeasure : model.getAllMeasures()) {
            for (ParameterDesc parameter : allMeasure.getFunction().getParameters()) {
                if (parameter.isConstantParameterDesc()) {
                    continue;
                }

                if (parameter.getColRef().getColumnDesc().isComputedColumn()
                        && parameter.getColRef().getColumnDesc().getName().equals(cc.getColumnName())) {
                    return true;
                }
            }
        }
        return false;
    }
}

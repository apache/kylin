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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.recommendation.entity.CCRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.springframework.beans.BeanUtils;

import lombok.Data;
import lombok.val;

@Data
public class ContextOutput implements Serializable {

    public static void merge(AbstractContext targetContext, ContextOutput output) {
        BeanUtils.copyProperties(output, targetContext);
        for (val modelContextOutput : output.getModelContextOutputs()) {
            val modelContext = new AbstractContext.ModelContext(targetContext, null);
            BeanUtils.copyProperties(modelContextOutput, modelContext);
            targetContext.getModelContexts().add(modelContext);
        }
        // json serializer ignore sql field, reset it
        targetContext.getAccelerateInfoMap()
                .forEach((sql, accInfo) -> accInfo.getRelatedLayouts().forEach(relation -> relation.setSql(sql)));
    }

    public static ContextOutput from(AbstractContext sourceContext) {
        val output = new ContextOutput();
        BeanUtils.copyProperties(sourceContext, output);
        for (AbstractContext.ModelContext modelContext : sourceContext.getModelContexts()) {
            val modelOutput = new ModelContextOutput();
            if (modelContext.getTargetModel() != null) {
                List<ComputedColumnDesc> targetCCs = modelContext.getTargetModel().getComputedColumnDescs();
                modelContext.getTargetModel().setComputedColumnUuids(
                        targetCCs.stream().map(ComputedColumnDesc::getUuid).collect(Collectors.toList()));
            }
            BeanUtils.copyProperties(modelContext, modelOutput);
            output.getModelContextOutputs().add(modelOutput);
        }
        return output;
    }

    private Map<String, AccelerateInfo> accelerateInfoMap = Maps.newHashMap();

    private List<ModelContextOutput> modelContextOutputs = Lists.newArrayList();

    @Data
    public static class ModelContextOutput implements Serializable {
        private NDataModel targetModel; // output model
        private NDataModel originModel; // used when update existing models

        private IndexPlan targetIndexPlan;
        private IndexPlan originIndexPlan;

        private Map<String, CCRecItemV2> ccRecItemMap = Maps.newHashMap();
        private Map<String, DimensionRecItemV2> dimensionRecItemMap = Maps.newHashMap();
        private Map<String, MeasureRecItemV2> measureRecItemMap = Maps.newHashMap();
        private Map<String, LayoutRecItemV2> indexRexItemMap = Maps.newHashMap();

        private boolean snapshotSelected;

        private Map<String, ComputedColumnDesc> usedCC = Maps.newHashMap();
    }
}

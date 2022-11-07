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
package org.apache.kylin.rest.model;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.ref.OptRecV2;
import org.apache.kylin.metadata.recommendation.ref.RecommendationRef;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class FuzzyKeySearcher {

    private FuzzyKeySearcher() {
    }

    public static Set<String> searchComputedColumns(NDataModel model, String key) {

        return model.getComputedColumnDescs().stream()
                .filter(cc -> StringUtils.containsIgnoreCase(cc.getFullName(), key)
                        || StringUtils.containsIgnoreCase(cc.getInnerExpression(), key))
                .map(ComputedColumnDesc::getFullName).collect(Collectors.toSet());
    }

    public static Set<Integer> searchDimensions(NDataModel model, Set<String> ccNameSet, String key) {
        return model.getAllNamedColumns().stream() //
                .filter(NDataModel.NamedColumn::isExist) //
                .filter(c -> StringUtils.containsIgnoreCase(c.getAliasDotColumn(), key)
                        || StringUtils.containsIgnoreCase(c.getName(), key)
                        || ccNameSet.contains(c.getAliasDotColumn()))
                .map(NDataModel.NamedColumn::getId) //
                .collect(Collectors.toSet());
    }

    public static Set<Integer> searchMeasures(NDataModel model, Set<String> ccNameSet, String key) {
        return model.getAllMeasures().stream() //
                .filter(m -> !m.isTomb())
                .filter(m -> StringUtils.containsIgnoreCase(m.getName(), key)
                        || m.getFunction().getParameters().stream()
                                .anyMatch(p -> p.getType().equals(FunctionDesc.PARAMETER_TYPE_COLUMN)
                                        && (StringUtils.containsIgnoreCase(p.getValue(), key)
                                                || ccNameSet.contains(p.getValue()))))
                .map(NDataModel.Measure::getId).collect(Collectors.toSet());
    }

    public static Set<Integer> searchColumnRefs(OptRecV2 recommendation, Set<String> ccFullNameSet, String key) {
        Set<Integer> recIds = Sets.newHashSet();
        recommendation.getColumnRefs().forEach((id, colRef) -> {
            if (StringUtils.containsIgnoreCase(colRef.getName(), key) //
                    || String.valueOf(colRef.getId()).equals(key) //
                    || ccFullNameSet.contains(colRef.getName())) {
                recIds.add(id);
            }
        });
        return recIds;
    }

    public static Set<Integer> searchCCRecRefs(OptRecV2 recommendation, String key) {
        Set<Integer> recIds = Sets.newHashSet();
        recommendation.getCcRefs().forEach((id, ref) -> {
            RawRecItem rawRecItem = recommendation.getRawRecItemMap().get(-id);
            ComputedColumnDesc cc = RawRecUtil.getCC(rawRecItem);
            if (StringUtils.containsIgnoreCase(cc.getFullName(), key)
                    || StringUtils.containsIgnoreCase(cc.getInnerExpression(), key)) {
                recIds.add(id);
                return;
            }
            for (int dependID : rawRecItem.getDependIDs()) {
                if (StringUtils.equalsIgnoreCase(String.valueOf(dependID), key)) {
                    recIds.add(id);
                    return;
                }
            }
        });
        return recIds;
    }

    public static Set<Integer> searchDependRefIds(OptRecV2 recommendation, Set<Integer> dependRefIds, String key) {
        Map<Integer, RecommendationRef> map = Maps.newHashMap();
        recommendation.getDimensionRefs().forEach(map::putIfAbsent);
        recommendation.getMeasureRefs().forEach(map::putIfAbsent);
        Set<Integer> recIds = Sets.newHashSet();
        map.forEach((id, ref) -> {
            if (StringUtils.containsIgnoreCase(ref.getName(), key)
                    || (ref.getId() >= 0 && String.valueOf(ref.getId()).equals(key))) {
                recIds.add(id);
                return;
            }
            for (RecommendationRef dependency : ref.getDependencies()) {
                if (dependRefIds.contains(dependency.getId())) {
                    recIds.add(id);
                    return;
                }
            }
        });
        return recIds;
    }
}

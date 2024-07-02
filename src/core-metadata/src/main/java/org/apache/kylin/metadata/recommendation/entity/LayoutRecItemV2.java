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

package org.apache.kylin.metadata.recommendation.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class LayoutRecItemV2 extends RecItemV2 implements Serializable {
    @JsonProperty("layout")
    private LayoutEntity layout;
    @JsonProperty("is_agg")
    private boolean isAgg;

    @Override
    public String getUniqueContent() {
        return layout.genUniqueContent();
    }

    public String getUniqueId(String modelId) {
        return modelId + "@" + layout.getId();
    }

    public int[] genDependIds() {
        List<Integer> colOrder = layout.getColOrder();
        int[] arr = new int[colOrder.size()];
        for (int i = 0; i < colOrder.size(); i++) {
            arr[i] = colOrder.get(i);
        }
        return arr;
    }

    @Override
    public int[] genDependIds(Map<String, RawRecItem> nonLayoutUniqueFlagRecMap, String content, NDataModel dataModel) {
        return genDependIds();
    }

    public void updateLayoutContent(NDataModel dataModel, Map<String, RawRecItem> nonLayoutUniqueFlagRecMap,
            Set<String> newCcUuids) {
        Map<String, ComputedColumnDesc> ccMap = dataModel.getCcMap();
        Map<String, RawRecItem> uniqueContentRecMap = Maps.newHashMap();
        nonLayoutUniqueFlagRecMap.forEach((uniqueFlag, recItem) -> {
            if (recItem.getModelID().equalsIgnoreCase(dataModel.getUuid())) {
                uniqueContentRecMap.put(recItem.getRecEntity().getUniqueContent(), recItem);
            }
        });

        ImmutableList<Integer> originColOrder = layout.getColOrder();
        List<Integer> originShardCols = layout.getShardByColumns();
        List<Integer> originSortCols = layout.getSortByColumns();
        List<Integer> originPartitionCols = layout.getPartitionByColumns();
        List<Integer> colOrderInDB = getColIDInDB(ccMap, newCcUuids, dataModel, originColOrder, uniqueContentRecMap);
        List<Integer> shardColsInDB = getColIDInDB(ccMap, newCcUuids, dataModel, originShardCols, uniqueContentRecMap);
        List<Integer> sortColsInDB = getColIDInDB(ccMap, newCcUuids, dataModel, originSortCols, uniqueContentRecMap);
        List<Integer> partitionColsInDB = getColIDInDB(ccMap, newCcUuids, dataModel, originPartitionCols,
                uniqueContentRecMap);
        layout.setColOrder(colOrderInDB);
        layout.setShardByColumns(shardColsInDB);
        layout.setPartitionByColumns(partitionColsInDB);
        log.debug("Origin colOrder is {}, converted to {}", originColOrder, colOrderInDB);
        log.debug("Origin shardBy columns is {}, converted to {}", originShardCols, shardColsInDB);
        log.debug("Origin sortBy columns is {}, converted to {}", originSortCols, sortColsInDB);
        log.debug("Origin partition columns is {}, converted to {}", originPartitionCols, partitionColsInDB);
    }

    private List<Integer> getColIDInDB(Map<String, ComputedColumnDesc> ccNameMap, Set<String> newCcUuids,
            NDataModel model, List<Integer> columnIDs, Map<String, RawRecItem> uniqueContentToRecItemMap) {
        List<Integer> colOrderInDB = Lists.newArrayListWithCapacity(columnIDs.size());
        columnIDs.forEach(colId -> {
            String uniqueContent;
            if (colId < NDataModel.MEASURE_ID_BASE) {
                TblColRef tblColRef = model.getEffectiveCols().get(colId);
                uniqueContent = RawRecUtil.dimensionUniqueContent(tblColRef, ccNameMap, newCcUuids);
            } else {
                NDataModel.Measure measure = model.getEffectiveMeasures().get(colId);
                uniqueContent = RawRecUtil.measureUniqueContent(measure, ccNameMap, newCcUuids);
            }
            if (uniqueContentToRecItemMap.containsKey(uniqueContent)) {
                colOrderInDB.add(-uniqueContentToRecItemMap.get(uniqueContent).getId());
            } else {
                colOrderInDB.add(colId);
            }
        });
        return colOrderInDB;
    }
}

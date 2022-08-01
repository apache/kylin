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

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

    public int[] genDependIds() {
        List<Integer> colOrder = layout.getColOrder();
        int[] arr = new int[colOrder.size()];
        for (int i = 0; i < colOrder.size(); i++) {
            arr[i] = colOrder.get(i);
        }
        return arr;
    }

    public void updateLayoutContent(NDataModel dataModel, Map<String, RawRecItem> nonLayoutUniqueFlagRecMap) {
        Map<String, ComputedColumnDesc> ccMap = getCcNameMapOnModel(dataModel);
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
        List<Integer> colOrderInDB = getColIDInDB(ccMap, dataModel, originColOrder, uniqueContentRecMap);
        List<Integer> shardColsInDB = getColIDInDB(ccMap, dataModel, originShardCols, uniqueContentRecMap);
        List<Integer> sortColsInDB = getColIDInDB(ccMap, dataModel, originSortCols, uniqueContentRecMap);
        List<Integer> partitionColsInDB = getColIDInDB(ccMap, dataModel, originPartitionCols, uniqueContentRecMap);
        layout.setColOrder(colOrderInDB);
        layout.setShardByColumns(shardColsInDB);
        layout.setPartitionByColumns(partitionColsInDB);
        log.debug("Origin colOrder is {}, converted to {}", originColOrder, colOrderInDB);
        log.debug("Origin shardBy columns is {}, converted to {}", originShardCols, shardColsInDB);
        log.debug("Origin sortBy columns is {}, converted to {}", originSortCols, sortColsInDB);
        log.debug("Origin partition columns is {}, converted to {}", originPartitionCols, partitionColsInDB);
    }

    private List<Integer> getColIDInDB(Map<String, ComputedColumnDesc> ccNameMap, NDataModel model,
            List<Integer> columnIDs, Map<String, RawRecItem> uniqueContentToRecItemMap) {
        List<Integer> colOrderInDB = Lists.newArrayListWithCapacity(columnIDs.size());
        columnIDs.forEach(colId -> {
            String uniqueContent;
            if (colId < NDataModel.MEASURE_ID_BASE) {
                TblColRef tblColRef = model.getEffectiveCols().get(colId);
                uniqueContent = RawRecUtil.dimensionUniqueContent(tblColRef, ccNameMap);
            } else {
                NDataModel.Measure measure = model.getEffectiveMeasures().get(colId);
                uniqueContent = RawRecUtil.measureUniqueContent(measure, ccNameMap);
            }
            if (uniqueContentToRecItemMap.containsKey(uniqueContent)) {
                colOrderInDB.add(-uniqueContentToRecItemMap.get(uniqueContent).getId());
            } else {
                colOrderInDB.add(colId);
            }
        });
        return colOrderInDB;
    }

    private Map<String, ComputedColumnDesc> getCcNameMapOnModel(NDataModel model) {
        Map<String, ComputedColumnDesc> ccMap = Maps.newHashMap();
        model.getComputedColumnDescs().forEach(cc -> {
            String aliasDotName = cc.getTableAlias() + "." + cc.getColumnName();
            ccMap.putIfAbsent(aliasDotName, cc);
        });
        return ccMap;
    }
}

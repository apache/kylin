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

package org.apache.kylin.rest.response;

import static org.apache.kylin.metadata.model.NDataModel.MEASURE_ID_BASE;

import java.util.Set;

import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BuildBaseIndexResponse extends BasicResponse {

    public static final BuildBaseIndexResponse EMPTY = new BuildBaseIndexResponse();

    @JsonProperty("base_table_index")
    private IndexInfo tableIndex;

    @JsonProperty("base_agg_index")
    private IndexInfo aggIndex;

    @JsonIgnore
    private boolean isCleanSecondStorage = false;

    public static BuildBaseIndexResponse from(IndexPlan indexPlan) {
        BuildBaseIndexResponse response = new BuildBaseIndexResponse();
        response.addLayout(indexPlan.getBaseAggLayout());
        response.addLayout(indexPlan.getBaseTableLayout());
        return response;
    }

    public void addLayout(LayoutEntity baseLayout) {
        if (baseLayout == null) {
            return;
        }
        IndexInfo info = new IndexInfo();

        info.setDimCount((int) baseLayout.getColOrder().stream().filter(id -> id < MEASURE_ID_BASE).count());
        info.setMeasureCount((int) baseLayout.getColOrder().stream().filter(id -> id >= MEASURE_ID_BASE).count());

        if (IndexEntity.isTableIndex(baseLayout.getId())) {
            tableIndex = info;
        } else {
            aggIndex = info;
        }
        info.setLayoutId(baseLayout.getId());
    }

    public void judgeIndexOperateType(boolean previousExist, boolean isAgg) {
        IndexInfo index = isAgg ? aggIndex : tableIndex;
        if (index != null) {
            if (previousExist) {
                index.setOperateType(OperateType.UPDATE);
            } else {
                index.setOperateType(OperateType.CREATE);
            }
        }
    }

    public void setIndexUpdateType(Set<Long> ids) {
        for (long id : ids) {
            if (IndexEntity.isAggIndex(id) && aggIndex != null) {
                aggIndex.setOperateType(OperateType.UPDATE);
            }
            if (IndexEntity.isTableIndex(id) && tableIndex != null) {
                tableIndex.setOperateType(OperateType.UPDATE);
            }
        }
    }

    public boolean hasIndexChange() {
        return tableIndex != null || aggIndex != null;
    }

    public boolean hasTableIndexChange() {
        return tableIndex != null;
    }

    @Data
    private static class IndexInfo {

        @JsonProperty("dimension_count")
        private int dimCount;

        @JsonProperty("measure_count")
        private int measureCount;

        @JsonProperty("layout_id")
        private long layoutId;

        @JsonProperty("operate_type")
        private OperateType operateType = OperateType.CREATE;
    }

    private enum OperateType {
        UPDATE, CREATE
    }
}

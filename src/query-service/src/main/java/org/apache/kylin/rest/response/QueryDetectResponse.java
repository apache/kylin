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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.query.QueryHistory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonAutoDetect(fieldVisibility = NONE, getterVisibility = NONE, isGetterVisibility = NONE, setterVisibility = NONE)
public class QueryDetectResponse {
    @JsonProperty("is_exception")
    private boolean isException = false;

    @JsonProperty("exception_message")
    private String exceptionMessage;

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("is_push_down")
    private boolean isPushDown = false;

    @JsonProperty("is_post_aggregation")
    private boolean isPostAggregation = false;

    @JsonProperty("is_table_index")
    private boolean isTableIndex = false;

    @JsonProperty("is_base_index")
    private boolean isBaseIndex = false;

    @JsonProperty("is_cache")
    private boolean isCache = false;

    @JsonProperty("is_constants")
    private boolean isConstants = false;

    @JsonProperty("realizations")
    private List<IndexInfo> realizations = Lists.newArrayList();

    public QueryDetectResponse buildExceptionResponse(SQLResponse sqlResponse) {
        this.isException = true;
        this.exceptionMessage = sqlResponse.getExceptionMessage();
        this.queryId = sqlResponse.getQueryId();
        this.isCache = sqlResponse.isHitExceptionCache();
        this.realizations = Lists.newArrayList();
        return this;
    }

    public QueryDetectResponse buildResponse(String project, SQLResponse sqlResponse, QueryContext queryContext) {
        // build RealizationVO
        List<QueryDetectResponse.IndexInfo> indexInfoList = sqlResponse.getNativeRealizations().stream()
                .map(realization -> new QueryDetectResponse.IndexInfo().buildResponse(realization, project))
                .collect(Collectors.toList());
        boolean isConstantQuery = QueryHistory.EngineType.CONSTANTS.name().equals(sqlResponse.getEngineType());

        this.isException = sqlResponse.isException;
        this.exceptionMessage = sqlResponse.getExceptionMessage();
        this.queryId = sqlResponse.getQueryId();
        this.isPushDown = sqlResponse.isQueryPushDown();
        this.isPostAggregation = !sqlResponse.isQueryPushDown() && !isConstantQuery
                && !queryContext.getMetrics().isExactlyMatch();
        // any realization is TableIndex、baseIndex
        this.isTableIndex = indexInfoList.stream().anyMatch(QueryDetectResponse.IndexInfo::isTableIndex);
        this.isBaseIndex = indexInfoList.stream().anyMatch(QueryDetectResponse.IndexInfo::isBaseIndex);
        this.isCache = sqlResponse.isStorageCacheUsed();
        this.isConstants = isConstantQuery;
        this.realizations = indexInfoList;
        return this;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonAutoDetect(fieldVisibility = NONE, getterVisibility = NONE, isGetterVisibility = NONE, setterVisibility = NONE)
    public static class IndexInfo {
        @JsonProperty("model_id")
        private String modelId;

        @JsonProperty("model_alias")
        private String modelAlias;

        @JsonProperty("layout_id")
        private long layoutId;

        @JsonProperty("index_type")
        private String indexType;

        @JsonProperty("partial_match_model")
        private boolean partialMatchModel = false;

        @JsonProperty("valid")
        private boolean valid = true;

        @JsonProperty("is_table_index")
        private boolean isTableIndex = false;

        @JsonProperty("is_base_index")
        private boolean isBaseIndex = false;

        public IndexInfo buildResponse(NativeQueryRealization realization, String project) {
            // calculate isBaseIndex
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            IndexPlan indexPlan = NIndexPlanManager.getInstance(kylinConfig, project)
                    .getIndexPlan(realization.getModelId());
            LayoutEntity layoutEntity = indexPlan == null ? null : indexPlan.getLayoutEntity(realization.getLayoutId());

            this.modelId = realization.getModelId();
            this.modelAlias = realization.getModelAlias();
            this.layoutId = realization.getLayoutId();
            this.indexType = realization.getType();
            this.partialMatchModel = realization.isPartialMatchModel();
            this.valid = realization.isValid();
            this.isTableIndex = IndexEntity.isTableIndex(realization.getLayoutId());
            this.isBaseIndex = layoutEntity != null && layoutEntity.isBaseIndex();
            return this;
        }
    }
}

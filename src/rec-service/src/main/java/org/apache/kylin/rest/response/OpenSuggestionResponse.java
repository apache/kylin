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

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OpenSuggestionResponse implements Serializable {

    @JsonProperty("models")
    private List<OpenModelRecResponse> models = Lists.newArrayList();

    @JsonProperty("error_sqls")
    private List<String> errorSqlList = Lists.newArrayList();

    @JsonProperty("optimal_models")
    private List<OpenModelRecResponse> optimalModels = Lists.newArrayList();

    public static List<OpenModelRecResponse> convert(List<SuggestionResponse.ModelRecResponse> response) {
        return response.stream().map(OpenModelRecResponse::convert).collect(Collectors.toList());
    }

    public static OpenSuggestionResponse from(SuggestionResponse innerResponse, List<String> sqls) {
        OpenSuggestionResponse result = new OpenSuggestionResponse();
        result.getModels().addAll(OpenSuggestionResponse.convert(innerResponse.getReusedModels().stream()
                .filter(e -> CollectionUtils.isNotEmpty(e.getIndexes())).collect(Collectors.toList())));
        result.getModels().addAll(OpenSuggestionResponse.convert(innerResponse.getNewModels()));
        if (CollectionUtils.isNotEmpty(innerResponse.getOptimalModels())) {
            result.getOptimalModels().addAll(OpenSuggestionResponse.convert(innerResponse.getOptimalModels()));
        }
        result.fillErrorSqlList(sqls);
        return result;
    }

    private void fillErrorSqlList(List<String> inputSqlList) {
        Set<String> normalRecommendedSqlSet = Sets.newHashSet();
        for (OpenModelRecResponse modelResponse : getModels()) {
            modelResponse.getIndexes().forEach(layoutRecDetailResponse -> {
                List<String> sqlList = layoutRecDetailResponse.getSqlList();
                normalRecommendedSqlSet.addAll(sqlList);
            });
        }

        for (OpenModelRecResponse modelResponse : getOptimalModels()) {
            modelResponse.getIndexes().forEach(layoutRecDetailResponse -> {
                List<String> sqlList = layoutRecDetailResponse.getSqlList();
                normalRecommendedSqlSet.addAll(sqlList);
            });
        }

        for (String sql : inputSqlList) {
            if (!normalRecommendedSqlSet.contains(sql)) {
                getErrorSqlList().add(sql);
            }
        }
    }
}

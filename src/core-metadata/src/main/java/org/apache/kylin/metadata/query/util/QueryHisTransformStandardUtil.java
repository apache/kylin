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

package org.apache.kylin.metadata.query.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryHistoryInfoResponse;
import org.apache.kylin.metadata.query.QueryHistoryResponse;
import org.apache.kylin.metadata.query.QueryHistorySql;
import org.apache.kylin.metadata.query.QueryRealization;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class QueryHisTransformStandardUtil {

    private static final String QUERY_HISTORIES = "query_histories";
    private static final String SIZE = "size";

    public static Map<String, Object> transformQueryHistory(Map<String, Object> queryHistories) {
        HashMap<String, Object> data = new HashMap<>();
        data.put(SIZE, queryHistories.get(SIZE));
        List<QueryHistoryResponse> queryHistoryResponses = Lists.newArrayList();
        if (queryHistories.get(QUERY_HISTORIES) == null) {
            return data;
        }
        List<QueryHistory> queryHistoriesList = (List<QueryHistory>)queryHistories.get(QUERY_HISTORIES);
        for (QueryHistory qh : queryHistoriesList) {
            QueryHistoryResponse history = new QueryHistoryResponse();
            history.setQueryRealizations(qh.getQueryRealizations());
            QueryHistorySql queryHistorySql = qh.getQueryHistorySql();
            history.setSql(queryHistorySql.getSqlWithParameterBindingComment());
            history.setQueryTime(qh.getQueryTime());
            history.setDuration(qh.getDuration());
            history.setHostName(qh.getHostName());
            history.setQuerySubmitter(qh.getQuerySubmitter());
            history.setQueryStatus(qh.getQueryStatus());
            history.setQueryId(qh.getQueryId());
            history.setId(qh.getId());
            history.setTotalScanCount(qh.getTotalScanCount());
            history.setTotalScanBytes(qh.getTotalScanBytes());
            history.setResultRowCount(qh.getResultRowCount());
            history.setCacheHit(qh.isCacheHit());
            history.setIndexHit(qh.isIndexHit());
            history.setEngineType(qh.getEngineType());
            history.setProjectName(qh.getProjectName());
            history.setErrorType(qh.getErrorType());
            history.setNativeQueryRealizations(transformQueryHistoryRealization(qh.getNativeQueryRealizations()));
            history.setQueryHistoryInfo(transformQueryHisInfo(qh.getQueryHistoryInfo()));
            queryHistoryResponses.add(history);
        }
        data.put(QUERY_HISTORIES, queryHistoryResponses);
        return data;
    }

    public static List<QueryRealization> transformQueryHistoryRealization(List<NativeQueryRealization> realizations) {
        List<QueryRealization> queryRealizations = Lists.newArrayList();
        if (realizations != null) {
            for (NativeQueryRealization r : realizations) {
                QueryRealization qr = new QueryRealization(
                        r.getModelId(), r.getModelAlias(), r.getLayoutId(), r.getIndexType(),
                        r.isPartialMatchModel(), r.isValid(), r.getSnapshots());
                queryRealizations.add(qr);
            }
        }
        return queryRealizations;
    }

    public static QueryHistoryInfoResponse transformQueryHisInfo(QueryHistoryInfo qh) {
        if (qh == null) {
            return null;
        }
        QueryHistoryInfoResponse queryHistoryInfoResponse = new QueryHistoryInfoResponse(
                qh.isExactlyMatch(), qh.getScanSegmentNum(), qh.getState(), qh.isExecutionError(), qh.getErrorMsg(),
                qh.getQuerySnapshots(), qh.getRealizationMetrics(), qh.getTraces(), qh.getCacheType(), qh.getQueryMsg());
        return queryHistoryInfoResponse;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> transformQueryHistorySqlForDisplay(Map<String, Object> querHistoryMap) {
        Map<String, Object> data = new HashMap<>();
        data.put(SIZE, querHistoryMap.get(SIZE));

        Object queryHistoryListObject;
        if ((queryHistoryListObject = querHistoryMap.get(QUERY_HISTORIES)) == null) {
            return data;
        }

        List<QueryHistory> queryHistoryList = (List<QueryHistory>) queryHistoryListObject;
        data.put(QUERY_HISTORIES, queryHistoryList.stream().map(qh -> {
            QueryHistorySql queryHistorySql = qh.getQueryHistorySql();
            qh.setSql(queryHistorySql.getSqlWithParameterBindingComment());
            return qh;
        }).collect(Collectors.toList()));

        return data;
    }
}

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

package org.apache.kylin.rest;

import static org.apache.kylin.metadata.query.QueryMetrics.QUERY_RESPONSE_TIME;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.guava30.shaded.common.base.Strings;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.rest.service.QueryHistoryScheduler;
import org.apache.spark.sql.SparderEnv;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Order(-200)
public class QueryInterceptor implements HandlerInterceptor {
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
            throws Exception {
        long start = System.currentTimeMillis();
        String queryExecutionID = QueryContext.current().getExecutionID();
        if (!Strings.isNullOrEmpty(queryExecutionID)) {
            SparderEnv.deleteQueryTaskResultBlock(queryExecutionID);
        }
        long responseStartTime = QueryContext.current().getResponseStartTime();
        if (responseStartTime > 0) {
            String queryId = QueryContext.current().getQueryId();
            long responseTime = System.currentTimeMillis() - responseStartTime;
            log.info("Query[{}] record QUERY_RESPONSE_TIME [{}]", queryId, responseTime);
            QueryMetrics queryMetrics = new QueryMetrics(queryId);
            QueryHistoryInfo info = new QueryHistoryInfo();
            info.getTraces().add(new QueryHistoryInfo.QueryTraceSpan(QUERY_RESPONSE_TIME, null, responseTime));
            queryMetrics.setQueryHistoryInfo(info);
            queryMetrics.setUpdateMetrics(true);
            QueryHistoryScheduler queryHistoryScheduler = QueryHistoryScheduler.getInstance();
            queryHistoryScheduler.offerQueryHistoryQueue(queryMetrics);
        }
        QueryContext.current().close();
        log.debug("QueryInterceptor use time [{}]ms", System.currentTimeMillis() - start);
    }
}

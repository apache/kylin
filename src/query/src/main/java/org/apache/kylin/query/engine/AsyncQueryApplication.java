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

package org.apache.kylin.query.engine;

import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_PROJECT_NAME;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_CONTEXT;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_ID;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_PARAMS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.query.util.AsyncQueryUtil;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.metadata.query.QueryHistorySql;
import org.apache.kylin.metadata.query.QueryHistorySqlParam;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.query.util.QueryHistoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

public class AsyncQueryApplication extends SparkApplication {

    protected static final Logger logger = LoggerFactory.getLogger(AsyncQueryApplication.class);

    @Override
    protected void handleException(Exception e) throws Exception {
        try {
            QueryContext.current().getMetrics().setException(true);
            AsyncQueryUtil.createErrorFlag(getParam(P_PROJECT_NAME), getParam(P_QUERY_ID), e.getMessage());
        } catch (Exception ex) {
            logger.error("save async query exception message failed");
        }
        throw e;
    }

    @Override
    protected void doExecute() throws IOException {
        logger.info("start async query job");
        QueryContext queryContext = null;
        QueryParams queryParams = null;
        try {
            queryContext = JsonUtil.readValue(getParam(P_QUERY_CONTEXT), QueryContext.class);
            QueryContext.set(queryContext);
            QueryMetricsContext.start(queryContext.getQueryId(), "");
            QueryRoutingEngine queryRoutingEngine = new QueryRoutingEngine();
            queryParams = JsonUtil.readValue(getParam(P_QUERY_PARAMS), QueryParams.class);
            queryParams.setKylinConfig(KylinConfig.getInstanceFromEnv());
            queryRoutingEngine.queryWithSqlMassage(queryParams);
            saveQueryHistory(queryContext, queryParams);
        } catch (Exception e) {
            logger.error("async query job failed.", e);
            if (queryContext != null && queryParams != null) {
                queryContext.getMetrics().setException(true);
                AsyncQueryUtil.createErrorFlag(getParam(P_PROJECT_NAME), getParam(P_QUERY_ID), e.getMessage());
                saveQueryHistory(queryContext, queryParams);
            }
        } finally {
            QueryMetricsContext.reset();
        }
    }

    @Override
    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        return config.getAsyncQuerySparkConfigOverride();
    }

    private void saveQueryHistory(QueryContext queryContext, QueryParams queryParams) {
        if (StringUtils.isEmpty(queryContext.getMetrics().getCorrectedSql())) {
            queryContext.getMetrics().setCorrectedSql(queryContext.getUserSQL());
        }
        try {
            QueryMetricsContext queryMetricsContext = QueryMetricsContext.collect(queryContext);
            queryMetricsContext.setSql(constructQueryHistorySqlText(queryParams, queryContext.getUserSQL()));
            // KE-36662 Using sql_pattern as normalized_sql storage
            String normalizedSql = QueryContext.currentMetrics().getCorrectedSql();
            queryMetricsContext.setSqlPattern(normalizedSql);

            RDBMSQueryHistoryDAO.getInstance().insert(queryMetricsContext);
        } catch (Exception e) {
            logger.error("async query job, save query history failed", e);
        }
    }

    private String constructQueryHistorySqlText(QueryParams queryParams, String originalSql)
            throws JsonProcessingException, ClassNotFoundException {

        List<QueryHistorySqlParam> params = null;
        if (queryParams.isPrepareStatementWithParams()) {
            params = new ArrayList<>();
            PrepareSqlStateParam[] requestParams = queryParams.getParams();
            for (int i = 0; i < requestParams.length; i++) {
                PrepareSqlStateParam p = requestParams[i];
                String dataType = QueryHistoryUtil.toDataType(p.getClassName());
                QueryHistorySqlParam param = new QueryHistorySqlParam(i + 1, p.getClassName(), dataType, p.getValue());
                params.add(param);
            }
        }

        // KE-36662 Do not store normalized_sql in sql_text, as it may exceed storage limitation
        return QueryHistoryUtil.toQueryHistorySqlText(new QueryHistorySql(originalSql, null, params));
    }

    public static void main(String[] args) {
        AsyncQueryApplication job = new AsyncQueryApplication();
        job.execute(args);
    }
}

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

package org.apache.kylin.rest.util;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.engine.PrepareSqlStateParam;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;
import org.apache.kylin.query.util.TempStatementUtil;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryUtils {

    private static final Logger logger = LoggerFactory.getLogger(QueryUtils.class);

    public static SQLResponse handleTempStatement(SQLRequest sqlRequest, KylinConfig config) {
        String sql = sqlRequest.getSql();
        Pair<Boolean, String> result = TempStatementUtil.handleTempStatement(sql, config);
        boolean isCreateTempStatement = result.getFirst();
        sql = result.getSecond();
        sqlRequest.setSql(sql);
        return isCreateTempStatement ? new SQLResponse(null, null, 0, false, null) : null;
    }

    public static boolean isPrepareStatementWithParams(SQLRequest sqlRequest) {
        return sqlRequest instanceof PrepareSqlRequest && ((PrepareSqlRequest) sqlRequest).getParams() != null
                && ((PrepareSqlRequest) sqlRequest).getParams().length > 0;
    }

    public static void fillInPrepareStatParams(SQLRequest sqlRequest, boolean pushdown) {
        KylinConfig kylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getProject(sqlRequest.getProject()).getConfig();
        if (QueryUtils.isPrepareStatementWithParams(sqlRequest)
                && !(kylinConfig.enableReplaceDynamicParams() || pushdown)) {
            PrepareSqlStateParam[] params = ((PrepareSqlRequest) sqlRequest).getParams();
            String filledSql = QueryContext.current().getMetrics().getCorrectedSql();
            try {
                filledSql = PrepareSQLUtils.fillInParams(filledSql, params);
            } catch (IllegalStateException e) {
                logger.error(e.getMessage(), e);
            }
            QueryContext.current().getMetrics().setCorrectedSql(filledSql);
        }
    }

    public static void updateQueryContextSQLMetrics(String alternativeSql) {
        QueryContext queryContext = QueryContext.current();
        if (StringUtils.isEmpty(queryContext.getMetrics().getCorrectedSql())
                && queryContext.getQueryTagInfo().isStorageCacheUsed()) {
            String defaultSchema = "DEFAULT";
            try {
                defaultSchema = new QueryExec(queryContext.getProject(), KylinConfig.getInstanceFromEnv())
                        .getDefaultSchemaName();
            } catch (Exception e) {
                logger.warn("Failed to get connection, project: {}", queryContext.getProject(), e);
            }
            QueryParams queryParams = new QueryParams(NProjectManager.getProjectConfig(queryContext.getProject()),
                    alternativeSql, queryContext.getProject(), queryContext.getLimit(), queryContext.getOffset(),
                    defaultSchema, false);
            queryParams.setAclInfo(queryContext.getAclInfo());
            queryContext.getMetrics().setCorrectedSql(QueryUtil.massageSql(queryParams));
        }
        if (StringUtils.isEmpty(queryContext.getMetrics().getCorrectedSql())) {
            queryContext.getMetrics().setCorrectedSql(alternativeSql);
        }
        queryContext.getMetrics().setSqlPattern(queryContext.getMetrics().getCorrectedSql());
    }
}

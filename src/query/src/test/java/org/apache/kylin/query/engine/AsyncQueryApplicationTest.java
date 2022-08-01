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

import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_CONTEXT;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_QUERY_PARAMS;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.query.util.QueryParams;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class AsyncQueryApplicationTest {

    private AsyncQueryApplication asyncQueryApplication;

    @Before
    public void setUp() throws Exception {
        asyncQueryApplication = spy(new AsyncQueryApplication());
    }

    @Test
    public void testDoExecute() throws Exception {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class);
                MockedStatic<RDBMSQueryHistoryDAO> rdbmsQueryHistoryDAOMockedStatic = mockStatic(
                        RDBMSQueryHistoryDAO.class);
                MockedStatic<QueryMetricsContext> queryMetricsContextMockedStatic = mockStatic(
                        QueryMetricsContext.class);
                MockedConstruction<QueryRoutingEngine> queryRoutingEngineMockedConstruction = mockConstruction(
                        QueryRoutingEngine.class)) {
            doReturn("{\"queryId\": \"query_uuid1\"}").when(asyncQueryApplication).getParam(P_QUERY_CONTEXT);
            doReturn("{}").when(asyncQueryApplication).getParam(P_QUERY_PARAMS);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(mock(KylinConfig.class));
            rdbmsQueryHistoryDAOMockedStatic.when(RDBMSQueryHistoryDAO::getInstance)
                    .thenReturn(mock(RDBMSQueryHistoryDAO.class));
            queryMetricsContextMockedStatic.when(() -> QueryMetricsContext.collect(any()))
                    .thenReturn(mock(QueryMetricsContext.class));

            asyncQueryApplication.doExecute();
        }
    }

    @Test
    public void testDoExecuteWithException() throws Exception {
        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = mockStatic(KylinConfig.class);
             MockedStatic<RDBMSQueryHistoryDAO> rdbmsQueryHistoryDAOMockedStatic = mockStatic(
                     RDBMSQueryHistoryDAO.class);
             MockedStatic<QueryMetricsContext> queryMetricsContextMockedStatic = mockStatic(
                     QueryMetricsContext.class);
             MockedConstruction<QueryRoutingEngine> queryRoutingEngineMockedConstruction = mockConstruction(
                     QueryRoutingEngine.class)) {
            doReturn("{\"queryId\": \"query_uuid1\"}").when(asyncQueryApplication).getParam(P_QUERY_CONTEXT);
            doReturn("xxx").when(asyncQueryApplication).getParam(P_QUERY_PARAMS);
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(mock(KylinConfig.class));
            rdbmsQueryHistoryDAOMockedStatic.when(RDBMSQueryHistoryDAO::getInstance)
                    .thenReturn(mock(RDBMSQueryHistoryDAO.class));
            queryMetricsContextMockedStatic.when(() -> QueryMetricsContext.collect(any()))
                    .thenReturn(mock(QueryMetricsContext.class));

            asyncQueryApplication.doExecute();
        }
    }

    @Test
    public void testConstructQueryHistorySqlText() {
        try (MockedStatic<QueryContext> queryContextMockedStatic = mockStatic(QueryContext.class)) {
            QueryContext.Metrics metrics = mock(QueryContext.Metrics.class);
            queryContextMockedStatic.when(QueryContext::currentMetrics).thenReturn(metrics);
            QueryParams queryParams = mock(QueryParams.class);
            when(queryParams.isPrepareStatementWithParams()).thenReturn(true);
            PrepareSqlStateParam prepareSqlStateParam = new PrepareSqlStateParam("java.lang.Integer", "1001");
            when(queryParams.getParams()).thenReturn(new PrepareSqlStateParam[] { prepareSqlStateParam });

            String result = (String) ReflectionTestUtils.invokeMethod(asyncQueryApplication,
                    "constructQueryHistorySqlText", queryParams, "-- comment\nselect col1 from table1");
            assertEquals(
                    "{\"sql\":\"-- comment\\nselect col1 from table1\",\"normalized_sql\":null,\"params\":[{\"pos\":1,\"java_type\":\"java.lang.Integer\",\"data_type\":\"INTEGER\",\"value\":\"1001\"}]}",
                    result);
        }
    }
}

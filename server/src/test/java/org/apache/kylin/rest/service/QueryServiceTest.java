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

package org.apache.kylin.rest.service;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

/**
 * @author xduo
 */
public class QueryServiceTest extends ServiceTestBase {

    @Autowired
    @Qualifier("queryService")
    QueryService queryService;

    @Autowired
    CacheManager cacheManager;

    @Test
    public void testBasics() throws JobException, IOException, SQLException {
        Assert.assertNotNull(queryService.getConfig());
        Assert.assertNotNull(queryService.getConfig());
        Assert.assertNotNull(queryService.getDataModelManager());
        Assert.assertNotNull(QueryConnection.getConnection(ProjectInstance.DEFAULT_PROJECT_NAME));

        //        Assert.assertTrue(queryService.getQueries("ADMIN").size() == 0);
        //
        //        queryService.saveQuery("test", "test", "select * from test_table", "test");
        //        Assert.assertTrue(queryService.getQueries("ADMIN").size() == 1);
        //
        //        queryService.removeQuery(queryService.getQueries("ADMIN").get(0).getProperty("id"));
        //        Assert.assertTrue(queryService.getQueries("ADMIN").size() == 0);

        SQLRequest request = new SQLRequest();
        request.setSql("select * from test_table");
        request.setAcceptPartial(true);
        QueryContext queryContext = QueryContextFacade.current();
        SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        queryService.logQuery(queryContext.getQueryId(), request, response);
    }

    @Test
    public void testCreateTableToWith() {
        String create_table1 = " create table tableId as select * from some_table1;";
        String create_table2 = "CREATE TABLE tableId2 AS select * FROM some_table2;";
        String select_table = "select * from tableId join tableId2 on tableId.a = tableId2.b;";

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.convert-create-table-to-with", "true");
        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {

            SQLRequest request = new SQLRequest();
            request.setProject("default");
            request.setSql(create_table1);
            queryService.doQueryWithCache(request);

            request.setSql(create_table2);
            queryService.doQueryWithCache(request);

            request.setSql(select_table);
            SQLResponse response = queryService.doQueryWithCache(request, true);

            Assert.assertEquals(
                    "WITH tableId as (select * from some_table1) , tableId2 AS (select * FROM some_table2) select * from tableId join tableId2 on tableId.a = tableId2.b;",
                    response.getExceptionMessage());
        }
    }

    @Test
    public void testSyntaxError() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.query.cache-enabled", "true");
        config.setProperty("kylin.query.lazy-query-enabled", "true");

        String badSql = "select with syntax error";

        SQLRequest request = new SQLRequest();
        request.setProject("default");
        request.setSql(badSql);

        try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            queryService.doQueryWithCache(request, false);
        } catch (Exception e) {
            // expected error
            Cache.ValueWrapper wrapper = cacheManager.getCache(QueryService.QUERY_CACHE).get(request.getCacheKey());
            Assert.assertTrue(wrapper == null || wrapper.get() == null);
        }
    }

    @Test
    public void testClassName() throws ClassNotFoundException {
        Assert.assertEquals(Class.forName("java.lang.Object"), queryService.getValidClass("java.io.DataInputStream"));
        Assert.assertEquals(Class.forName("java.lang.String"), queryService.getValidClass("java.lang.String"));
    }
}

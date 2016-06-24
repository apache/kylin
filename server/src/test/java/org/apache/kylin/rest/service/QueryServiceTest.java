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

import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.request.SQLRequest;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author xduo
 */
public class QueryServiceTest extends ServiceTestBase {

    @Autowired
    QueryService queryService;

    @Autowired
    private CacheService cacheService;

    @Test
    public void testBasics() throws JobException, IOException, SQLException {
        Assert.assertNotNull(queryService.getConfig());
        Assert.assertNotNull(queryService.getConfig());
        Assert.assertNotNull(queryService.getMetadataManager());
        Assert.assertNotNull(cacheService.getOLAPDataSource(ProjectInstance.DEFAULT_PROJECT_NAME));

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
        SQLResponse response = new SQLResponse();
        response.setHitExceptionCache(true);
        queryService.logQuery(request, response);
    }
}

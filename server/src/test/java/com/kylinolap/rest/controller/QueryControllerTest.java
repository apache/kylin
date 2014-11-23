/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.rest.controller;

import net.sf.ehcache.CacheManager;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import com.kylinolap.cube.project.ProjectInstance;
import com.kylinolap.rest.request.MetaRequest;
import com.kylinolap.rest.request.SQLRequest;
import com.kylinolap.rest.service.QueryService;
import com.kylinolap.rest.service.ServiceTestBase;
import com.kylinolap.rest.util.QueryUtil;

/**
 * @author xduo
 * 
 */
public class QueryControllerTest extends ServiceTestBase {

    private QueryController queryController;
    @Autowired
    QueryService queryService;
    @Autowired
    private CacheManager cacheManager;

    @Before
    public void setup() {
        super.setUp();

        queryController = new QueryController();
        queryController.setQueryService(queryService);
        queryController.setCacheManager(cacheManager);
    }

    @Test(expected = Exception.class)
    public void testQueryException() throws Exception {
        SQLRequest sqlRequest = new SQLRequest();
        sqlRequest.setSql("select * from not_exist_table");
        sqlRequest.setProject("default");
        queryController.query(sqlRequest);
    }

    @Test
    public void testErrorMsg() {
        String errorMsg = "error while executing SQL \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\": From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'";
        assert QueryUtil.makeErrorMsgUserFriendly(errorMsg).equals("From line 14, column 14 to line 14, column 29: Column 'CLSFD_GA_PRFL_ID' not found in table 'LKP'\n" + "while executing SQL: \"select lkp.clsfd_ga_prfl_id, ga.sum_dt, sum(ga.bounces) as bounces, sum(ga.exits) as exits, sum(ga.entrances) as entrances, sum(ga.pageviews) as pageviews, count(distinct ga.GA_VSTR_ID, ga.GA_VST_ID) as visits, count(distinct ga.GA_VSTR_ID) as uniqVistors from CLSFD_GA_PGTYPE_CATEG_LOC ga left join clsfd_ga_prfl_lkp lkp on ga.SRC_GA_PRFL_ID = lkp.SRC_GA_PRFL_ID group by lkp.clsfd_ga_prfl_id,ga.sum_dt order by lkp.clsfd_ga_prfl_id,ga.sum_dt LIMIT 50000\"");
    }

    @Test
    public void testGetMetadata() {
        queryController.getMetadata(new MetaRequest(ProjectInstance.DEFAULT_PROJECT_NAME));
    }

}

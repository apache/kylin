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

package org.apache.kylin.tool;

import java.util.List;

import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHistoryAccessCLI {

    private static final Logger logger = LoggerFactory.getLogger(QueryHistoryAccessCLI.class);
    private static final String PROJECT = "test_project";
    private static final String FAIL_LOG = "query history access test failed.";
    private final RDBMSQueryHistoryDAO queryHistoryDAO;

    public QueryHistoryAccessCLI() {
        this.queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
    }

    public static void main(String[] args) {
        QueryHistoryAccessCLI cli = null;
        try {
            cli = new QueryHistoryAccessCLI();
        } catch (Exception e) {
            logger.error("Test failed.", e);
            Unsafe.systemExit(1);
        }

        if (args.length != 1) {
            Unsafe.systemExit(1);
        }

        long repetition = Long.parseLong(args[0]);

        while (repetition > 0) {
            if (!cli.testAccessQueryHistory()) {
                logger.error("Test failed.");
                Unsafe.systemExit(1);
            }
            repetition--;
        }

        logger.info("Test succeed.");
        Unsafe.systemExit(0);
    }

    public boolean testAccessQueryHistory() {
        try {
            QueryMetrics queryMetrics = new QueryMetrics("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1", "192.168.1.6:7070");
            queryMetrics.setSql("select LSTG_FORMAT_NAME from KYLIN_SALES\nLIMIT 500");
            queryMetrics.setSqlPattern("SELECT \"LSTG_FORMAT_NAME\"\nFROM \"KYLIN_SALES\"\nLIMIT 1");
            queryMetrics.setQueryDuration(5578L);
            queryMetrics.setTotalScanBytes(863L);
            queryMetrics.setTotalScanCount(4096L);
            queryMetrics.setResultRowCount(500L);
            queryMetrics.setSubmitter("ADMIN");
            queryMetrics.setErrorType("");
            queryMetrics.setCacheHit(true);
            queryMetrics.setIndexHit(true);
            queryMetrics.setQueryTime(1584888338274L);
            queryMetrics.setProjectName(PROJECT);
            QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(true, 5, true);

            QueryMetrics.RealizationMetrics realizationMetrics = new QueryMetrics.RealizationMetrics("20000000001L",
                    "Table Index", "771157c2-e6e2-4072-80c4-8ec25e1a83ea", Lists.newArrayList());
            realizationMetrics.setQueryId("6a9a151f-f992-4d52-a8ec-8ff3fd3de6b1");
            realizationMetrics.setDuration(4591L);
            realizationMetrics.setQueryTime(1586405449387L);
            realizationMetrics.setProjectName(PROJECT);

            List<QueryMetrics.RealizationMetrics> realizationMetricsList = Lists.newArrayList();
            realizationMetricsList.add(realizationMetrics);
            realizationMetricsList.add(realizationMetrics);
            queryHistoryInfo.setRealizationMetrics(realizationMetricsList);
            queryMetrics.setQueryHistoryInfo(queryHistoryInfo);
            queryHistoryDAO.insert(queryMetrics);

            // clean test data
            queryHistoryDAO.deleteQueryHistoryByProject(PROJECT);
            queryHistoryDAO.deleteAllQueryHistoryRealizationForProject(PROJECT);
        } catch (Exception e) {
            logger.error(FAIL_LOG, e);
            return false;
        }
        return true;
    }
}

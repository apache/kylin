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

package org.apache.kylin.engine.spark2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContextFacade;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.QueryConnection;
import org.apache.kylin.query.util.QueryInfoCollector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryInfoCollectorTest extends LocalWithSparkSessionTest {
    private Connection connection = null;
    private Statement statement = null;
    private ResultSet resultSet = null;

    @Before
    public void setUp() throws Exception {
        super.setup();
        System.setProperty("spark.local", "true");
    }

    @After
    public void after() {
        super.after();
    }

    @Test
    public void testQueryInfoCollector() throws Exception {
        prepareContexts();
        enableCube("ci_inner_join_cube", "ci_left_join_cube");

        try {
            connection = QueryConnection.getConnection("default");
            statement = connection.createStatement();
            String sql = "select count(*) as cnt1 from test_kylin_fact inner join test_account on seller_id = account_id\n" +
                    "union all\n" +
                    "select count(*) as cnt2 from test_kylin_fact left join test_account on seller_id = account_id";
            resultSet = statement.executeQuery(sql);

            Assert.assertNotNull(resultSet);

            List<String> cubes = QueryInfoCollector.current().getCubeNames();

            Assert.assertTrue(cubes.contains("CUBE[name=ci_inner_join_cube]"));
            Assert.assertTrue(cubes.contains("CUBE[name=ci_left_join_cube]"));
        } finally {
            cleanContexts();
            close();
        }
    }

    @Test
    public void testQueryInfoCollectorReset() throws Exception {
        prepareContexts();
        enableCube("ci_left_join_cube");

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            String project = "default";
            String expectedCube = "CUBE[name=ci_left_join_cube]";

            String sqlWithCube = "select count(*) from test_kylin_fact";
            FutureTask<String> queryTask1 = new FutureTask<String>(new QueryCallable(sqlWithCube, project, false));
            executorService.submit(queryTask1);

            String cubeName1 = queryTask1.get(2, TimeUnit.MINUTES);

            Assert.assertTrue(queryTask1.isDone());
            Assert.assertEquals(expectedCube, cubeName1);

            String sqlNoCube = "select * from test_account";
            FutureTask<String> queryTask2 = new FutureTask<String>(new QueryCallable(sqlNoCube, project, true));
            executorService.submit(queryTask2);

            String cubeName2 = queryTask2.get(2, TimeUnit.MINUTES);

            Assert.assertTrue(queryTask2.isDone());
            Assert.assertEquals(cubeName1, cubeName2);

            FutureTask<String> queryTask3 = new FutureTask<String>(new QueryCallable(sqlNoCube, project, true));
            executorService.submit(queryTask3);

            String cubeName3 = queryTask3.get(2, TimeUnit.MINUTES);

            Assert.assertTrue(queryTask3.isDone());
            Assert.assertEquals("", cubeName3);
        } finally {
            executorService.shutdown();
            cleanContexts();
        }
    }

    class QueryCallable implements Callable<String> {
        private String sql;
        private String project;
        private boolean reset;

        public QueryCallable(String sql, String project, boolean reset) {
            this.sql = sql;
            this.project = project;
            this.reset = reset;
        }

        @Override
        public String call() throws Exception {
            Connection connection = QueryConnection.getConnection(project);
            Statement statement = connection.createStatement();

            try {
                statement.executeQuery(sql);
                return QueryInfoCollector.current().getCubeNameString();
            } catch (Exception e) {
                return QueryInfoCollector.current().getCubeNameString();
            } finally {
                if (reset) {
                    QueryInfoCollector.reset();
                }
            }
        }
    }

    private void cleanContexts() {
        QueryContextFacade.resetCurrent();
        QueryInfoCollector.reset();
        BackdoorToggles.cleanToggles();
    }

    private void prepareContexts() {
        QueryContextFacade.resetCurrent();
        BackdoorToggles.addToggle(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");
    }

    private void enableCube(String... cubes) throws IOException {
        CubeManager cubeManager = CubeManager.getInstance(KylinConfig.getInstanceFromEnv());

        for (String cube : cubes) {
            CubeInstance cubeInstance = cubeManager.getCube(cube);
            cubeManager.updateCubeStatus(cubeInstance, RealizationStatusEnum.READY);
        }
    }

    private void close() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }
}

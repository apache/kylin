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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.exception.SchedulerException;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.KylinSparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.common.SparkQueryTest;
import org.junit.Assert;
import org.junit.Test;

public class BuildAndQueryEmptySegmentsTest extends LocalWithSparkSessionTest {

    private static final String CUBE_NAME = "ci_left_join_cube";
    private static final String SQL = "SELECT COUNT(1) as TRANS_CNT from TEST_KYLIN_FACT";
    protected KylinConfig config;
    protected CubeManager cubeMgr;
    protected ExecutableManager execMgr;

    @Override
    public void setup() throws SchedulerException {
        super.setup();
        config = KylinConfig.getInstanceFromEnv();
        cubeMgr = CubeManager.getInstance(config);
        execMgr = ExecutableManager.getInstance(config);
    }

    @Override
    public void after() {
        super.after();
    }

    @Test
    public void testEmptySegments() throws Exception {
        cleanupSegments(CUBE_NAME);
        buildCuboid(CUBE_NAME, new SegmentRange.TSRange(dateToLong("2009-01-01"), dateToLong("2009-06-01")));
        Assert.assertEquals(0, cubeMgr.getCube(CUBE_NAME).getSegments().get(0).getInputRecords());

        populateSSWithCSVData(config, getProject(), KylinSparkEnv.getSparkSession());

        testQueryUnequal(SQL);

        buildCuboid(CUBE_NAME, new SegmentRange.TSRange(dateToLong("2012-06-01"), dateToLong("2015-01-01")));
        Assert.assertNotEquals(0, cubeMgr.getCube(CUBE_NAME).getSegments().get(1).getInputRecords());

        testQuery(SQL);
    }

    private void testQuery(String sqlStr) {
        Pair<Dataset<Row>, NExecAndComp.ITQueryMetrics> pair = NExecAndComp.sql(getProject(), sqlStr);
        Assert.assertEquals(1L, pair.getFirst().count());

        Dataset dsFromSpark = NExecAndComp.querySparkSql(sqlStr);
        Assert.assertEquals(1L, dsFromSpark.count());
        String msg = SparkQueryTest.checkAnswer(pair.getFirst(), dsFromSpark, false);
        Assert.assertNotNull(msg);
    }

    private void testQueryUnequal(String sqlStr) {

        Pair<Dataset<Row>, NExecAndComp.ITQueryMetrics> pair = NExecAndComp.sql(getProject(), sqlStr);
        Assert.assertEquals(1L, pair.getFirst().count());

        Dataset dsFromSpark = NExecAndComp.querySparkSql(sqlStr);
        Assert.assertEquals(1L, dsFromSpark.count());
        String msg = SparkQueryTest.checkAnswer(pair.getFirst(), dsFromSpark, false);
        Assert.assertNotNull(msg);
    }

    private String convertToSparkSQL(String sqlStr) {
        return sqlStr.replaceAll("edw\\.", "");
    }

}

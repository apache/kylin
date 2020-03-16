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

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.kylin.engine.spark.LocalWithSparkSessionTest;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Dataset;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BuildAndQueryEmptySegmentsTest extends LocalWithSparkSessionTest {

    private static final String CUBE_NAME1 = "ci_inner_join_cube";

    private static final String SQL = "select\n" + " count(1) as TRANS_CNT \n" + " from test_kylin_fact \n"
            + " group by trans_id";

    private static final String SQL_DERIVED = "SELECT \n" + "test_cal_dt.season_beg_dt\n"
            + "FROM test_kylin_fact LEFT JOIN edw.test_cal_dt as test_cal_dt \n"
            + "ON test_kylin_fact.cal_dt=test_cal_dt.cal_dt \n"
            + "WHERE test_kylin_fact.cal_dt>'2009-06-01' and test_kylin_fact.cal_dt<'2013-01-01' \n"
            + "GROUP BY test_cal_dt.season_beg_dt";

    @Before
    public void init() throws Exception {
        super.init();
    }

    @After
    public void cleanup() {
        DefaultScheduler.destroyInstance();
        super.cleanupTestMetadata();
        System.clearProperty("kylin.job.scheduler.poll-interval-second");
    }

    @Test
    public void testEmptySegments() throws Exception {
        cleanupSegments(CUBE_NAME1);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        buildCube(CUBE_NAME1, f.parse("2009-01-01").getTime(), f.parse("2009-06-01").getTime());
        Assert.assertEquals(0, cubeMgr.getCube(CUBE_NAME1).getSegments().get(0).getInputRecords());

        testQueryUnequal(SQL);
        testQueryUnequal(SQL_DERIVED);

        buildCube(CUBE_NAME1, f.parse("2009-06-01").getTime(), f.parse("2010-01-01").getTime());
        Assert.assertEquals(0, cubeMgr.getCube(CUBE_NAME1).getSegments().get(1).getInputRecords());
        buildCube(CUBE_NAME1, f.parse("2010-01-01").getTime(), f.parse("2011-01-01").getTime());
        Assert.assertEquals(0, cubeMgr.getCube(CUBE_NAME1).getSegments().get(2).getInputRecords());
        buildCube(CUBE_NAME1, f.parse("2011-01-01").getTime(), f.parse("2015-01-01").getTime());
        Assert.assertNotEquals(0, cubeMgr.getCube(CUBE_NAME1).getSegments().get(3).getInputRecords());

        mergeSegments(CUBE_NAME1, f.parse("2009-01-01").getTime(), f.parse("2010-01-01").getTime(), true);
        mergeSegments(CUBE_NAME1, f.parse("2010-01-01").getTime(), f.parse("2015-01-01").getTime(), true);

        testQuery(SQL);
        testQuery(SQL_DERIVED);
    }

    private void buildCube(String cubeName, long start, long end) throws Exception {
        buildCuboid(cubeName, new SegmentRange.TSRange(start, end));
    }

    private void testQuery(String sqlStr) {
        Dataset dsFromCube = NExecAndComp.sql(getProject(), sqlStr);
        Assert.assertNotEquals(0L, dsFromCube.count());
        String sql = convertToSparkSQL(sqlStr);
        Dataset dsFromSpark = NExecAndComp.querySparkSql(sql);
        Assert.assertEquals(dsFromCube.count(), dsFromSpark.count());
    }

    private void testQueryUnequal(String sqlStr) {

        Dataset dsFromCube = NExecAndComp.sql(getProject(), sqlStr);
        Assert.assertEquals(0L, dsFromCube.count());
        String sql = convertToSparkSQL(sqlStr);
        Dataset dsFromSpark = NExecAndComp.querySparkSql(sql);
        Assert.assertNotEquals(dsFromCube.count(), dsFromSpark.count());
    }

    private String convertToSparkSQL(String sqlStr) {
        return sqlStr.replaceAll("edw\\.", "");
    }

}
